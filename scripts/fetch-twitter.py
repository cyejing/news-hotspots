#!/usr/bin/env python3
"""
Twitter / X 抓取脚本。

职责：
- 读取 `twitter.json` 中的 timeline 源配置
- 读取 `topics.json` 中的 Twitter 查询配置
- 抓取 timeline 与 topic query 两类结果
- 将两类结果统一标准化为 `source_type=twitter` 的 `articles`
- 将失败请求、耗时和慢请求统计写入 `*.meta.json`

执行逻辑：
1. 加载 runtime、twitter source 配置与 topic 配置
2. 共享同一套 cooldown，顺序执行 timeline 与 query 抓取
3. 成功结果进入统一 articles；失败请求只记入 meta
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存标准化后的 Twitter article
- `<step>.meta.json`
  只保存抓取诊断与失败明细
"""

import argparse
import html
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

try:
    from config_loader import load_merged_runtime_config, load_merged_twitter_sources, load_merged_topics
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config, load_merged_twitter_sources, load_merged_topics
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta

COOLDOWN_SECONDS = 7.0
DEFAULT_TIMEOUT = 180
DEFAULT_COUNT = 20
RESULTS_PER_QUERY = 10
TWITTER_DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"
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
    global COOLDOWN_SECONDS, DEFAULT_TIMEOUT, DEFAULT_COUNT, RESULTS_PER_QUERY
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("twitter", {})
    diagnostics_config = runtime.get("diagnostics", {})
    COOLDOWN_SECONDS = float(fetch_config.get("cooldown_s", COOLDOWN_SECONDS) or 0)
    DEFAULT_TIMEOUT = int(fetch_config.get("request_timeout_s", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    DEFAULT_COUNT = int(fetch_config.get("count", DEFAULT_COUNT) or DEFAULT_COUNT)
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


def load_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return [source for source in load_merged_twitter_sources(defaults_dir, config_dir) if source.get("enabled", True)]


def normalize_text(text: str) -> str:
    return " ".join(html.unescape(text or "").split())


def truncate_text(text: str, limit: int = 280) -> str:
    normalized = normalize_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def parse_twitter_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, TWITTER_DATE_FORMAT).astimezone(timezone.utc)
    except ValueError:
        return None


def within_hours(tweet_dt: Optional[datetime], cutoff: datetime) -> bool:
    if tweet_dt is None:
        return False
    return tweet_dt >= cutoff


def timeline_count_for_source(source: Dict[str, Any]) -> int:
    try:
        count = int(source.get("limit", DEFAULT_COUNT))
    except (TypeError, ValueError):
        count = DEFAULT_COUNT
    return max(1, count)


def result_count_for_topic(topic: Dict[str, Any]) -> int:
    return max(1, RESULTS_PER_QUERY)


def format_search_term(term: str, exclude: bool = False) -> str:
    value = normalize_text(term)
    if not value:
        return ""
    if " " in value:
        value = f"\"{value}\""
    return f"-{value}" if exclude else value


def build_twitter_query(base_query: str, exclude: List[str]) -> str:
    parts = [normalize_text(base_query)]
    parts.extend(
        formatted
        for formatted in (format_search_term(term, exclude=True) for term in exclude)
        if formatted
    )
    return " ".join(part for part in parts if part)


def extract_tweets(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    for key in ("tweets", "results", "items", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            for nested_key in ("tweets", "results", "items"):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, list):
                    return [item for item in nested_value if isinstance(item, dict)]
    return []


def parse_tweet(item: Dict[str, Any], topic_id: str, cutoff: datetime, query: Optional[str] = None) -> Optional[Dict[str, Any]]:
    text = truncate_text(item.get("text", item.get("full_text", "")))
    link = item.get("url", item.get("link", ""))
    if not text or not link:
        return None

    tweet_dt = parse_twitter_datetime(item.get("created_at", item.get("createdAt", "")))
    if not within_hours(tweet_dt, cutoff):
        return None

    likes = int(item.get("likes", item.get("like_count", 0)) or 0)
    retweets = int(item.get("retweets", item.get("retweet_count", 0)) or 0)
    replies = int(item.get("replies", item.get("reply_count", 0)) or 0)
    quotes = int(item.get("quotes", item.get("quote_count", 0)) or 0)
    impressions = item.get("impressions", item.get("impression_count"))

    article = {
        "title": text,
        "link": link,
        "date": tweet_dt.isoformat(),
        "topic": topic_id,
        "summary": text,
        "metrics": {
            "like_count": likes,
            "retweet_count": retweets,
            "reply_count": replies,
            "quote_count": quotes,
            "impression_count": impressions,
        },
        "tweet_id": item.get("id"),
        "tweet_type": item.get("type"),
        "author": item.get("author") or item.get("username"),
        "rt_author": item.get("rt_author"),
    }
    if query:
        article["twitter_query"] = query
    return article


def fetch_timeline(source: Dict[str, Any]) -> Dict[str, Any]:
    handle = source.get("handle")
    if not handle:
        raise ValueError(f"Twitter source missing handle: {source.get('id', 'unknown')}")
    count = timeline_count_for_source(source)
    return run_bb_browser_site(["twitter/tweets", handle, str(count)])


def fetch_source(source: Dict[str, Any], cutoff: datetime) -> Dict[str, Any]:
    started_at = time.monotonic()
    try:
        payload = fetch_timeline(source)
        articles = []
        for item in extract_tweets(payload):
            article = parse_tweet(item, str(source.get("topic") or ""), cutoff)
            if article:
                articles.append(article)
        elapsed_s = time.monotonic() - started_at
        request_trace = build_request_trace(
            source.get("handle") or source.get("id", "unknown"),
            elapsed_s,
            status="ok",
            backend="bb-browser",
            adapter="twitter/tweets",
        )
        return {
            "source_id": source.get("id"),
            "source_type": "twitter",
            "name": source.get("name", source.get("id", "unknown")),
            "handle": source.get("handle"),
            "priority": source.get("priority", 3),
            "topic": str(source.get("topic") or ""),
            "status": "ok",
            "attempts": 1,
            "elapsed_s": round(elapsed_s, 3),
            "items": len(articles),
            "count": len(articles),
            "articles": articles,
            "request_traces": [request_trace],
            "failed_items": [],
        }
    except Exception as exc:
        elapsed_s = time.monotonic() - started_at
        request_trace = build_request_trace(
            source.get("handle") or source.get("id", "unknown"),
            elapsed_s,
            status="error",
            backend="bb-browser",
            adapter="twitter/tweets",
            error=str(exc)[:200],
        )
        return {
            "source_id": source.get("id"),
            "source_type": "twitter",
            "name": source.get("name", source.get("id", "unknown")),
            "handle": source.get("handle"),
            "priority": source.get("priority", 3),
            "topic": str(source.get("topic") or ""),
            "status": "error",
            "attempts": 1,
            "error": str(exc)[:200],
            "elapsed_s": round(elapsed_s, 3),
            "items": 0,
            "count": 0,
            "articles": [],
            "request_traces": [request_trace],
            "failed_items": [normalize_failed_item(source.get("id"), str(exc)[:200], elapsed_s)],
        }


def fetch_topic(topic: Dict[str, Any], cutoff: datetime, logger: logging.Logger) -> Dict[str, Any]:
    search = topic.get("search", {})
    queries = search.get("twitter_queries", [])
    exclude = search.get("exclude", [])
    per_query = result_count_for_topic(topic)

    dedup_by_url: Dict[str, Dict[str, Any]] = {}
    request_traces: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    failed_items: List[Dict[str, Any]] = []
    ok_queries = 0

    for query in queries:
        compiled_query = build_twitter_query(query, exclude)
        query_started_at = time.monotonic()
        try:
            payload = run_bb_browser_site(["twitter/search", compiled_query, str(per_query), "latest"])
            tweets = extract_tweets(payload)
            kept = 0
            for item in tweets:
                article = parse_tweet(item, topic.get("id"), cutoff, compiled_query)
                if not article:
                    continue
                dedup_by_url.setdefault(article["link"], article)
                kept += 1
            elapsed_s = time.monotonic() - query_started_at
            request_traces.append(build_request_trace(compiled_query, elapsed_s, status="ok", backend="bb-browser", adapter="twitter/search"))
            ok_queries += 1
        except Exception as exc:
            logger.warning("Twitter query failed [%s]: %s", topic.get("id"), exc)
            elapsed_s = time.monotonic() - query_started_at
            request_traces.append(build_request_trace(compiled_query, elapsed_s, status="error", backend="bb-browser", adapter="twitter/search", error=str(exc)[:200]))
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
        description="Fetch Twitter/X timelines and topic-query search results via bb-browser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-twitter.py --defaults config/defaults --config workspace/config --hours 48 --output twitter.json
    python3 fetch-twitter.py --output twitter.json --verbose
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--hours", type=int, default=48, help="Time window in hours")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-twitter-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        sources = load_sources(args.defaults, effective_config_dir)
        topics = load_merged_topics(args.defaults, effective_config_dir)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
        step_started_at = time.monotonic()
        logger.info("Fetching %d Twitter sources and %d topic query groups sequentially", len(sources), len(topics))
        logger.info("Twitter bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)

        source_results = [fetch_source(source, cutoff) for source in sources]
        for result in source_results:
            if result["status"] == "ok":
                logger.info("✅ %s: %d tweets", result["name"], result["count"])
            else:
                logger.warning("❌ %s: %s", result["name"], result.get("error"))

        topic_results = [fetch_topic(topic, cutoff, logger) for topic in topics if topic.get("search", {}).get("twitter_queries")]
        ok_sources = sum(1 for result in source_results if result["status"] == "ok")
        ok_topics = sum(1 for result in topic_results if result["status"] == "ok")
        total_query_calls = sum(int(result.get("calls_total", 0) or 0) for result in topic_results)
        ok_query_calls = sum(int(result.get("calls_ok", 0) or 0) for result in topic_results)
        total_articles = sum(result.get("count", 0) for result in source_results) + sum(result.get("count", 0) for result in topic_results)
        total_calls = len(source_results) + total_query_calls
        ok_calls = ok_sources + ok_query_calls
        articles = [article for result in source_results for article in result.get("articles", []) if isinstance(article, dict)]
        articles.extend(article for result in topic_results for article in result.get("articles", []) if isinstance(article, dict))
        failed_items = [item for result in [*source_results, *topic_results] for item in result.get("failed_items", []) if isinstance(item, dict)]
        request_traces = [trace for result in [*source_results, *topic_results] for trace in result.get("request_traces", []) if isinstance(trace, dict)]

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "twitter",
            "articles": articles,
        }
        meta = build_step_meta(
            step_key="twitter",
            status="ok" if ok_calls == total_calls and total_articles > 0 else ("partial" if ok_calls > 0 and total_articles > 0 else "error"),
            elapsed_s=time.monotonic() - step_started_at,
            items=total_articles,
            calls_total=total_calls,
            calls_ok=ok_calls,
            failed_items=failed_items,
            request_traces=request_traces,
        )
        write_result_with_meta(args.output, output, meta)

        logger.info(
            "✅ Done: %d/%d sources ok, %d/%d query groups ok, %d tweets → %s",
            ok_sources,
            len(source_results),
            ok_topics,
            len(topic_results),
            total_articles,
            args.output,
        )
        return 0 if total_articles > 0 else 1
    except Exception as exc:
        logger.error("💥 Twitter fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
