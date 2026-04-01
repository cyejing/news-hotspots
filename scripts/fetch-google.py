#!/usr/bin/env python3
"""
Google News 抓取脚本。

职责：
- 读取 `topics.json` 中的 Google News 查询配置
- 逐个 query 调用 `bb-browser site google/news`
- 将搜索结果标准化为 `source_type=google` 的统一 `articles`
- 将失败请求、耗时和慢请求统计写入 `*.meta.json`

执行逻辑：
1. 加载 runtime 与 topics 配置
2. 逐个 topic、逐个 query 顺序请求 Google News
3. 成功结果立即标准化；失败请求跳过但保留失败明细
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存抓到的 article 数据，给 `merge-sources.py` 合并
- `<step>.meta.json`
  只保存抓取诊断，不承担结果表达职责
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

try:
    from config_loader import load_merged_runtime_config, load_merged_topics
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config, load_merged_topics
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta

COOLDOWN_SECONDS = 12.0
DEFAULT_TIMEOUT = 180
RESULTS_PER_QUERY = 10
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
    global COOLDOWN_SECONDS, DEFAULT_TIMEOUT, RESULTS_PER_QUERY
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("google", {})
    diagnostics_config = runtime.get("diagnostics", {})
    COOLDOWN_SECONDS = float(fetch_config.get("cooldown_s", COOLDOWN_SECONDS) or 0)
    DEFAULT_TIMEOUT = int(fetch_config.get("request_timeout_s", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    RESULTS_PER_QUERY = int(fetch_config.get("results_per_query", RESULTS_PER_QUERY) or RESULTS_PER_QUERY)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def run_bb_browser_site(args: Sequence[str], timeout: Optional[int] = None) -> Dict[str, Any]:
    global _last_success_at
    throttle_after_success()
    effective_timeout = int(timeout if timeout is not None else DEFAULT_TIMEOUT)
    result = subprocess.run(
        ["bb-browser", "site", *args],
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


def normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def result_count_for_topic(topic: Dict[str, Any]) -> int:
    return max(1, RESULTS_PER_QUERY)


def format_google_term(term: str, exclude: bool = False) -> str:
    value = normalize_text(term)
    if not value:
        return ""
    if " " in value:
        value = f"\"{value}\""
    return f"-{value}" if exclude else value


def build_google_query(base_query: str, exclude: List[str]) -> str:
    parts = [normalize_text(base_query)]
    parts.extend(
        formatted
        for formatted in (format_google_term(term, exclude=True) for term in exclude)
        if formatted
    )
    return " ".join(part for part in parts if part)


def fetch_topic(topic: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    search = topic.get("search", {})
    queries = list(search.get("google_queries", []))
    exclude = search.get("exclude", [])
    per_query = result_count_for_topic(topic)

    dedup_by_url: Dict[str, Dict[str, Any]] = {}
    request_traces: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    failed_items: List[Dict[str, Any]] = []
    ok_queries = 0

    for query in queries:
        compiled_query = build_google_query(query, exclude)
        query_started_at = time.monotonic()
        try:
            payload = run_bb_browser_site(["google/news", compiled_query, str(per_query)])
            results = payload.get("results", [])
            kept = 0
            for item in results:
                article = {
                    "title": normalize_text(item.get("title", "")),
                    "link": item.get("url", ""),
                    "snippet": normalize_text(item.get("snippet", "")),
                    "date": datetime.fromtimestamp(
                        item.get("timestamp", time.time()),
                        tz=timezone.utc,
                    ).isoformat(),
                    "topic": topic.get("id"),
                    "publisher": normalize_text(item.get("source", "")),
                    "google_query": compiled_query,
                }
                if not article["title"] or not article["link"]:
                    continue
                dedup_by_url.setdefault(article["link"], article)
                kept += 1
            elapsed_s = time.monotonic() - query_started_at
            request_traces.append(build_request_trace(compiled_query, elapsed_s, status="ok", backend="bb-browser", adapter="google/news"))
            ok_queries += 1
        except Exception as exc:
            logger.warning("Google News query failed [%s]: %s", topic.get("id"), exc)
            elapsed_s = time.monotonic() - query_started_at
            request_traces.append(build_request_trace(compiled_query, elapsed_s, status="error", backend="bb-browser", adapter="google/news", error=str(exc)[:200]))
            failed_items.append(normalize_failed_item(compiled_query, str(exc)[:200], elapsed_s))

    articles = list(dedup_by_url.values())
    articles.sort(key=lambda article: article.get("date", ""), reverse=True)
    return {
        "topic_id": topic.get("id"),
        "status": "ok" if articles else "error",
        "elapsed_s": round(time.monotonic() - started_at, 3),
        "calls_total": len(queries),
        "calls_ok": ok_queries,
        "failed_items": failed_items,
        "request_traces": request_traces,
        "items": len(articles),
        "count": len(articles),
        "articles": articles,
    }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Google News results via bb-browser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-google.py --defaults config/defaults --config workspace/config --output google.json
    python3 fetch-google.py --output google.json --verbose
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Accepted for CLI consistency; not used by Google News fetch")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-google-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        topics = load_merged_topics(args.defaults, effective_config_dir)
        step_started_at = time.monotonic()
        logger.info("Fetching Google News for %d topics sequentially", len(topics))
        logger.info("Google bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
        topic_results = [fetch_topic(topic, logger) for topic in topics if topic.get("search", {}).get("google_queries")]
        ok_topics = sum(1 for result in topic_results if result["status"] == "ok")
        total_articles = sum(result.get("count", 0) for result in topic_results)
        total_queries = sum(int(result.get("calls_total", 0) or 0) for result in topic_results)
        ok_queries = sum(int(result.get("calls_ok", 0) or 0) for result in topic_results)
        articles = [article for result in topic_results for article in result.get("articles", []) if isinstance(article, dict)]
        failed_items = [item for result in topic_results for item in result.get("failed_items", []) if isinstance(item, dict)]
        request_traces = [trace for result in topic_results for trace in result.get("request_traces", []) if isinstance(trace, dict)]
        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "google",
            "articles": articles,
        }
        meta = build_step_meta(
            step_key="google",
            status="ok" if ok_queries == total_queries and total_articles > 0 else ("partial" if ok_queries > 0 and total_articles > 0 else "error"),
            elapsed_s=time.monotonic() - step_started_at,
            items=total_articles,
            calls_total=total_queries,
            calls_ok=ok_queries,
            failed_items=failed_items,
            request_traces=request_traces,
        )
        write_result_with_meta(args.output, output, meta)
        logger.info("✅ Done: %d/%d topics ok, %d articles → %s", ok_topics, len(topic_results), total_articles, args.output)
        return 0 if total_articles > 0 else 1
    except Exception as exc:
        logger.error("💥 Google fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
