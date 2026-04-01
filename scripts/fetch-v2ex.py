#!/usr/bin/env python3
"""
V2EX 热榜抓取脚本。

职责：
- 调用 `bb-browser site v2ex/hot` 获取热榜
- 将热榜条目标准化为统一 `articles`
- 将抓取耗时、失败信息和慢请求统计写入 `*.meta.json`

执行逻辑：
1. 加载 runtime 配置
2. 顺序执行 V2EX 抓取并遵守 cooldown
3. 将结果转换为统一 article 结构
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存抓到的 V2EX article
- `<step>.meta.json`
  只保存 step 级诊断与失败明细
"""

import argparse
import html
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

try:
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta

SOURCE_ID = "v2ex-api"
SOURCE_NAME = "V2EX Hot"
SOURCE_PRIORITY = 4
DEFAULT_TIMEOUT = 60
COOLDOWN_SECONDS = 5.0

_last_success_at: Optional[float] = None


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(__name__)


def throttle_after_success() -> None:
    global _last_success_at
    if _last_success_at is None:
        return
    wait_seconds = COOLDOWN_SECONDS - (time.monotonic() - _last_success_at)
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global DEFAULT_TIMEOUT, COOLDOWN_SECONDS
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("v2ex", {})
    diagnostics_config = runtime.get("diagnostics", {})
    DEFAULT_TIMEOUT = int(fetch_config.get("request_timeout_s", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    COOLDOWN_SECONDS = float(fetch_config.get("cooldown_s", COOLDOWN_SECONDS) or 0)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def run_bb_browser_site(command: Sequence[str], timeout: Optional[int] = None) -> Dict[str, Any]:
    global _last_success_at
    throttle_after_success()
    effective_timeout = int(timeout if timeout is not None else DEFAULT_TIMEOUT)

    result = subprocess.run(
        ["bb-browser", "site", *command],
        capture_output=True,
        text=True,
        timeout=effective_timeout,
        env=os.environ,
    )

    if result.returncode != 0:
        message = (result.stderr or result.stdout or "bb-browser command failed").strip()
        raise RuntimeError(message)

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from bb-browser: {exc}") from exc

    _last_success_at = time.monotonic()
    return payload


def clean_text(value: str) -> str:
    text = html.unescape(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def truncate_summary(value: str, limit: int = 240) -> str:
    text = clean_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def transform_topic(
    item: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    title = clean_text(item.get("title", ""))
    link = item.get("url", "")
    if not title or not link:
        return None

    content = item.get("content", "") or ""
    node_slug = (item.get("nodeSlug") or "").strip().lower()
    node_name = clean_text(item.get("node", ""))
    topic = "technology"

    created = item.get("created")
    date_iso = ""
    if isinstance(created, (int, float)):
        date_iso = datetime.fromtimestamp(created, tz=timezone.utc).isoformat()

    replies = int(item.get("replies", 0) or 0)
    author = clean_text(item.get("author", ""))
    summary = truncate_summary(content)

    return {
        "title": title,
        "link": link,
        "date": date_iso,
        "summary": summary,
        "topic": topic,
        "replies": replies,
        "author": author,
        "node": node_name,
        "node_slug": node_slug,
        "v2ex_id": item.get("id"),
    }


def fetch_v2ex_hot(
    logger: logging.Logger,
    defaults_dir: Optional[Path] = None,
    config_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    logger.info("V2EX bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
    started_at = time.monotonic()
    payload = run_bb_browser_site(["v2ex/hot"])
    elapsed_s = time.monotonic() - started_at
    request_trace = build_request_trace("v2ex/hot", "v2ex/hot", elapsed_s, status="ok", backend="bb-browser", adapter="v2ex/hot")
    raw_topics = payload.get("topics", [])
    articles: List[Dict[str, Any]] = []
    for item in raw_topics:
        article = transform_topic(item)
        if article:
            articles.append(article)

    logger.info(
        "Fetched V2EX hot topics: %d raw, %d kept",
        len(raw_topics),
        len(articles),
    )

    source_result = {
        "source_id": SOURCE_ID,
        "source_type": "v2ex",
        "name": SOURCE_NAME,
        "priority": SOURCE_PRIORITY,
        "topic": articles[0].get("topic", "technology") if articles else "technology",
        "status": "ok" if articles else "error",
        "elapsed_s": round(elapsed_s, 3),
        "items": len(articles),
        "count": len(articles),
        "fetched_count": len(raw_topics),
        "articles": articles,
        "request_traces": [request_trace],
    }
    if not articles:
        source_result["error"] = "No tech-relevant V2EX hot topics found"

    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "source_type": "v2ex",
        "calls_total": 1,
        "calls_ok": 1 if articles else 0,
        "items_total": len(articles),
        "sources_total": 1,
        "sources_ok": 1 if articles else 0,
        "total_articles": len(articles),
        "sources": [source_result],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch V2EX hot topics via bb-browser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-v2ex.py --defaults config/defaults --config workspace/config --output v2ex.json
    python3 fetch-v2ex.py --verbose
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Accepted for CLI consistency; not used by V2EX fetch")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-v2ex-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        data = fetch_v2ex_hot(logger, defaults_dir=args.defaults, config_dir=effective_config_dir)
        source_result = (data.get("sources") or [{}])[0]
        output = {
            "generated": data.get("generated"),
            "source_type": "v2ex",
            "articles": source_result.get("articles", []),
        }
        meta = build_step_meta(
            step_key="v2ex",
            status="ok" if int(data.get("calls_ok", 0) or 0) == int(data.get("calls_total", 1) or 1) and int(data.get("items_total", 0) or 0) > 0 else "error",
            elapsed_s=float(source_result.get("elapsed_s", 0) or 0),
            items=int(data.get("items_total", 0) or 0),
            calls_total=int(data.get("calls_total", 1) or 1),
            calls_ok=int(data.get("calls_ok", 0) or 0),
            failed_items=[] if source_result.get("status") == "ok" else [normalize_failed_item(SOURCE_ID, source_result.get("error"), source_result.get("elapsed_s"))],
            request_traces=source_result.get("request_traces", []),
        )
        write_result_with_meta(args.output, output, meta)
        logger.info(
            "✅ Done: %d/%d sources ok, %d articles → %s",
            data.get("sources_ok", 0),
            data.get("sources_total", 0),
            data.get("total_articles", 0),
            args.output,
        )
        return 0 if data.get("items_total", 0) else 1
    except Exception as exc:
        logger.error("💥 V2EX fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
