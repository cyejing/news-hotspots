#!/usr/bin/env python3
"""
TLDR newsletter 抓取脚本（HTML 抓取型源，非 RSS）。

职责：
- 拉取 tldr.tech/ai 与 tldr.tech/tech 最近一期的每日 issue 页面
- 解析 <article> 块（标题/链接/摘要），过滤 Sponsor 条目
- TLDR AI → ai-frontier，TLDR Tech → technology
- 输出统一 articles 契约 + sidecar meta

页面结构（2026-10-31 勘察）：
  <article class="mt-3">
    <a class="font-bold" href="URL"><h3>TITLE (N minute read)</h3></a>
    <div class="newsletter-html">SUMMARY</div>
  </article>
  分类区块头：<h3 class="text-center font-bold">Headlines &amp; Launches</h3>
  当日无 issue 时 URL 返回落地页（无 <article>），需回退前一天。
"""

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, local_now, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, local_now, write_result_with_meta

STEP_KEY = "tldr"
DEFAULT_TIMEOUT = 20
MAX_DATE_LOOKBACK_DAYS = 4

# (source_id, name, path, topic, priority)
TLDR_SOURCES = [
    {"source_id": "tldr-ai", "name": "TLDR AI", "path": "ai", "topic": "ai-frontier", "priority": 4},
    {"source_id": "tldr-tech", "name": "TLDR Tech", "path": "tech", "topic": "technology", "priority": 4},
]

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0 Safari/537.36"

ARTICLE_BLOCK_RE = re.compile(
    r'<article[^>]*>\s*<a[^>]+href="(https?://[^"]+)"[^>]*>\s*<h3[^>]*>(.*?)</h3>\s*</a>\s*'
    r'(?:<div[^>]*>(.*?)</div>)?',
    re.S,
)
# 新版结构（2026-11 起）：<article><div><h3><a href>TITLE</a></h3>...</div>...</article>
ARTICLE_BLOCK_RE_V2 = re.compile(
    r'<article[^>]*>.*?<h3[^>]*>\s*<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>\s*</h3>(.*?)</article>',
    re.S,
)
SPONSOR_MARK = "(Sponsor)"


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    return logging.getLogger(__name__)


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global DEFAULT_TIMEOUT
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get(STEP_KEY, {})
    diagnostics_config = runtime.get("diagnostics", {})
    DEFAULT_TIMEOUT = int(fetch_config.get("request_timeout_s", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def strip_tags(fragment: str) -> str:
    text = unescape(re.sub(r"<[^>]+>", " ", fragment or ""))
    return re.sub(r"\s+", " ", text).strip()


def http_get(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    with urlopen(request, timeout=DEFAULT_TIMEOUT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def find_latest_issue(path: str, today: datetime, logger: logging.Logger) -> Optional[str]:
    """从今天起回退找最近一期有内容的 issue，返回 HTML 或 None。"""
    for offset in range(MAX_DATE_LOOKBACK_DAYS):
        day = today - timedelta(days=offset)
        url = f"https://tldr.tech/{path}/{day:%Y-%m-%d}"
        started = time.monotonic()
        try:
            html_text = http_get(url)
        except Exception as exc:
            logger.warning("TLDR fetch %s failed: %s", url, exc)
            continue
        if "<article" in html_text:
            return html_text
        logger.debug("TLDR %s has no issue content (landing page), trying previous day", url)
    return None


def parse_articles(html_text: str, spec: Dict[str, Any], issue_date: datetime) -> List[Dict[str, Any]]:
    matches = list(ARTICLE_BLOCK_RE.finditer(html_text))
    # 新版结构：h3 内包 a，摘要在后续文本里（同块截取）
    if not matches:
        matches = [
            (m.group(1), m.group(2), m.group(3)[:800])
            for m in ARTICLE_BLOCK_RE_V2.finditer(html_text)
        ]
    else:
        matches = [(m.group(1), m.group(2), m.group(3) or "") for m in matches]

    articles: List[Dict[str, Any]] = []
    seen_links = set()
    for raw_url, raw_title, raw_summary in matches:
        title = strip_tags(raw_title)
        if not title or SPONSOR_MARK.lower() in title.lower():
            continue
        url = raw_url.split("?utm_source")[0].strip()
        if not url.startswith("http") or url in seen_links:
            continue
        if "tldr.tech" in urlparse(url).netloc:
            continue
        seen_links.add(url)
        articles.append(
            {
                "title": title,
                "link": url,
                "date": issue_date.isoformat(),
                "topic": spec["topic"],
                "summary": strip_tags(raw_summary)[:400],
                "source_name": spec["name"],
                "hot_score": 0,
                "metrics": {"issue_date": f"{issue_date:%Y-%m-%d}"},
            }
        )
    return articles


def fetch_tldr_source(spec: Dict[str, Any], today: datetime, logger: logging.Logger) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        html_text = find_latest_issue(spec["path"], today, logger)
    except Exception as exc:
        html_text = None
        error_msg = str(exc)[:200]
    else:
        error_msg = ""

    elapsed_s = time.monotonic() - started
    if not html_text:
        return {
            "source_id": spec["source_id"],
            "source_type": STEP_KEY,
            "name": spec["name"],
            "priority": spec["priority"],
            "topic": spec["topic"],
            "status": "error",
            "error": error_msg or "no recent issue found",
            "elapsed_s": round(elapsed_s, 3),
            "count": 0,
            "articles": [],
            "request_traces": [
                build_request_trace(
                    spec["source_id"], spec["path"], elapsed_s, status="error",
                    source_type=STEP_KEY, method="http", attempt=1, backend="http",
                    error=error_msg or "no recent issue found",
                )
            ],
        }

    articles = parse_articles(html_text, spec, today)
    logger.info("TLDR %s: %d articles", spec["name"], len(articles))
    return {
        "source_id": spec["source_id"],
        "source_type": STEP_KEY,
        "name": spec["name"],
        "priority": spec["priority"],
        "topic": spec["topic"],
        "status": "ok" if articles else "error",
        "error": "" if articles else "issue page had no articles",
        "elapsed_s": round(elapsed_s, 3),
        "count": len(articles),
        "articles": articles,
        "request_traces": [
            build_request_trace(
                spec["source_id"], spec["path"], elapsed_s, status="ok" if articles else "error",
                source_type=STEP_KEY, method="http", attempt=1, backend="http",
            )
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TLDR newsletter daily issues (AI + Tech).")
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--hours", type=int, default=48, help="Accepted for CLI consistency; TLDR issues are daily")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-tldr-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    started_at = time.monotonic()
    today = local_now().replace(hour=0, minute=0, second=0, microsecond=0)
    results = [fetch_tldr_source(spec, today, logger) for spec in TLDR_SOURCES]
    articles = [a for r in results for a in r["articles"]]
    traces = [t for r in results for t in r["request_traces"]]
    ok = sum(1 for r in results if r["status"] == "ok")

    meta = build_step_meta(
        step_key=STEP_KEY,
        status="ok" if ok == len(results) and articles else ("partial" if ok else "error"),
        elapsed_active_s=sum(r["elapsed_s"] for r in results),
        elapsed_total_s=time.monotonic() - started_at,
        items=len(articles),
        calls_total=len(results),
        calls_ok=ok,
        failed_items=None,
        request_traces=traces,
    )
    write_result_with_meta(args.output, {"generated": local_now().isoformat(), "source_type": STEP_KEY, "articles": articles}, meta)
    logger.info("✅ TLDR done: %d/%d newsletters, %d articles → %s", ok, len(results), len(articles), args.output)
    return 0 if articles else 1


if __name__ == "__main__":
    sys.exit(main())
