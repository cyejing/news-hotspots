#!/usr/bin/env python3
"""
Reddit 抓取脚本。

职责：
- 读取 `reddit.json` 中的 subreddit 源配置
- 读取 `topics.json` 中的 Reddit 查询配置
- 抓取 subreddit 与 topic query 两类结果
- 将两类结果统一标准化为 `source_type=reddit` 的 `articles`
- 将失败明细与耗时统一写入 `*.meta.json`

执行逻辑：
1. 加载 runtime、reddit source 配置与 topic 配置
2. 共享同一套 cooldown，顺序执行 source 抓取与 query 抓取
3. 成功结果写入统一 articles；失败请求只进入 meta
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存标准化后的 Reddit article
- `<step>.meta.json`
  只保存请求级诊断和 step 级汇总
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
    from config_loader import load_merged_runtime_config, load_merged_reddit_sources, load_merged_topics
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config, load_merged_reddit_sources, load_merged_topics
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta

COOLDOWN_SECONDS = 6.0
DEFAULT_TIMEOUT = 180
RESULTS_PER_QUERY = 10
_last_success_at: Optional[float] = None
_reddit_search_block_reason: Optional[str] = None


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
    fetch_config = runtime.get("fetch", {}).get("reddit", {})
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


def is_blocking_reddit_search_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "http 403" in message and "please log in to https://www.reddit.com" in message


def load_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return [source for source in load_merged_reddit_sources(defaults_dir, config_dir) if source.get("enabled", True)]


def hours_to_reddit_time(hours: int) -> str:
    if hours <= 48:
        return "day"
    else:
        return "week"

def result_count_for_topic(topic: Dict[str, Any]) -> int:
    return max(1, RESULTS_PER_QUERY)


def extract_posts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    for key in ("posts", "results", "items", "children", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for nested_key in ("children", "items", "posts"):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, list):
                    return nested_value
    return []


def parse_post(
        item: Dict[str, Any],
        topic_or_source: Any,
        min_score: int = 0,
        query: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if isinstance(topic_or_source, dict):
        topic_id = str(topic_or_source.get("topic") or "")
        min_score = int(topic_or_source.get("min_score", min_score) or 0)
    else:
        topic_id = str(topic_or_source or "")

    data = item.get("data") if isinstance(item.get("data"), dict) else item
    title = (data.get("title") or "").strip()
    if not title:
        return None

    permalink = data.get("permalink") or data.get("reddit_url") or data.get("url")
    if permalink and permalink.startswith("/"):
        reddit_url = f"https://www.reddit.com{permalink}"
    else:
        reddit_url = permalink or ""
    external_url = data.get("external_url")
    if not external_url:
        url = data.get("url") or ""
        external_url = url if not url.startswith("/r/") else None

    link = external_url or reddit_url
    if not link:
        return None

    created = data.get("created_utc")
    if created is None:
        created = data.get("created")
    date_iso = ""
    if isinstance(created, (int, float)):
        date_iso = datetime.fromtimestamp(created, tz=timezone.utc).isoformat()

    score = int(data.get("score", 0) or 0)
    num_comments = int(data.get("num_comments", data.get("comments", data.get("comment_count", 0))) or 0)
    if score < int(min_score or 0):
        return None

    flair = data.get("link_flair_text") or data.get("flair")
    is_self = bool(data.get("is_self", False))
    summary = (data.get("selftext") or data.get("text") or data.get("snippet") or "").strip()

    article = {
        "title": title,
        "link": link,
        "reddit_url": reddit_url,
        "external_url": external_url,
        "date": date_iso,
        "score": score,
        "num_comments": num_comments,
        "flair": flair,
        "is_self": is_self,
        "summary": summary[:400],
        "topic": topic_id,
        "metrics": {
            "score": score,
            "num_comments": num_comments,
            "upvote_ratio": data.get("upvote_ratio"),
        },
    }
    if query:
        article["reddit_query"] = query
    return article


def fetch_hot_source(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    subreddit = source.get("subreddit")
    limit = int(source.get("limit", 25) or 25)
    args = ["reddit/hot"]
    if subreddit:
        args.append(subreddit)
    args.append(str(limit))
    payload = run_bb_browser_site(args)
    return extract_posts(payload)


def fetch_source(source: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.monotonic()
    subreddit = source.get("subreddit")
    source_key = subreddit or source.get("id", "unknown")
    try:
        raw_posts = fetch_hot_source(source)
        articles = []
        for item in raw_posts:
            article = parse_post(item, str(source.get("topic") or ""), int(source.get("min_score", 0) or 0))
            if article:
                articles.append(article)
        elapsed_s = time.monotonic() - started_at
        request_trace = build_request_trace(
            source_key,
            elapsed_s,
            status="ok",
            backend="bb-browser",
            adapter="reddit/hot",
        )

        return {
            "source_id": source.get("id"),
            "source_type": "reddit",
            "name": source.get("name", source.get("id", "unknown")),
            "subreddit": subreddit,
            "sort": source.get("sort", "hot"),
            "mode": "hot",
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
            source_key,
            elapsed_s,
            status="error",
            backend="bb-browser",
            adapter="reddit/hot",
            error=str(exc)[:200],
        )
        return {
            "source_id": source.get("id"),
            "source_type": "reddit",
            "name": source.get("name", source.get("id", "unknown")),
            "subreddit": subreddit,
            "sort": source.get("sort", "hot"),
            "mode": "hot",
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


def fetch_topic(topic: Dict[str, Any], hours: int, logger: logging.Logger) -> Dict[str, Any]:
    global _reddit_search_block_reason
    search = topic.get("search", {})
    queries = search.get("reddit_queries", [])
    exclude = search.get("exclude", [])
    per_query = result_count_for_topic(topic)
    time_filter = hours_to_reddit_time(hours)

    dedup_by_url: Dict[str, Dict[str, Any]] = {}
    request_traces: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    failed_items: List[Dict[str, Any]] = []
    ok_queries = 0

    for query in queries:
        if _reddit_search_block_reason:
            logger.warning("Reddit query skipped [%s]: %s", topic.get("id"), _reddit_search_block_reason)
            break
        compiled_query = " ".join(
            [query] + [f'-"{term}"' if " " in term else f"-{term}" for term in exclude if str(term).strip()])
        query_started_at = time.monotonic()
        try:
            payload = run_bb_browser_site(["reddit/search", compiled_query, "top", time_filter, str(per_query)])
            posts = extract_posts(payload)
            kept = 0
            for item in posts:
                article = parse_post(item, topic.get("id"), 0, compiled_query)
                if not article:
                    continue
                dedup_by_url.setdefault(article["link"], article)
                kept += 1
            elapsed_s = time.monotonic() - query_started_at
            request_traces.append(build_request_trace(compiled_query, elapsed_s, status="ok", backend="bb-browser", adapter="reddit/search"))
            ok_queries += 1
        except Exception as exc:
            logger.warning("Reddit query failed [%s]: %s", topic.get("id"), exc)
            elapsed_s = time.monotonic() - query_started_at
            request_traces.append(build_request_trace(compiled_query, elapsed_s, status="error", backend="bb-browser", adapter="reddit/search", error=str(exc)[:200]))
            failed_items.append(normalize_failed_item(compiled_query, str(exc)[:200], elapsed_s))
            if is_blocking_reddit_search_error(exc):
                _reddit_search_block_reason = str(exc)[:200]
                break

    articles = list(dedup_by_url.values())
    articles.sort(key=lambda article: article.get("score", 0), reverse=True)
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
        description="Sequential Reddit fetcher via subreddits and topic queries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-reddit.py --defaults config/defaults --config workspace/config --hours 48 --output reddit.json
    python3 fetch-reddit.py --output reddit.json --verbose
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"),
                        help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--hours", type=int, default=48, help="Used for topic-query time mapping")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", action="store_true",
                        help="Accepted for CLI consistency; this fetcher always refreshes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-reddit-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        sources = load_sources(args.defaults, effective_config_dir)
        topics = load_merged_topics(args.defaults, effective_config_dir)
        logger.info("Fetching %d Reddit sources and %d topic query groups sequentially", len(sources), len(topics))
        logger.info("Reddit bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)

        source_results = [fetch_source(source) for source in sources]
        for result in source_results:
            if result["status"] == "ok":
                logger.info("✅ %s: %d posts", result["name"], result["count"])
            else:
                logger.warning("❌ %s: %s", result["name"], result.get("error"))

        topic_results = [fetch_topic(topic, args.hours, logger) for topic in topics if
                         topic.get("search", {}).get("reddit_queries")]
        ok_sources = sum(1 for result in source_results if result["status"] == "ok")
        ok_topics = sum(1 for result in topic_results if result["status"] == "ok")
        total_query_calls = sum(int(result.get("calls_total", 0) or 0) for result in topic_results)
        ok_query_calls = sum(int(result.get("calls_ok", 0) or 0) for result in topic_results)
        total_posts = sum(result.get("count", 0) for result in source_results) + sum(
            result.get("count", 0) for result in topic_results)
        total_calls = len(source_results) + total_query_calls
        ok_calls = ok_sources + ok_query_calls
        articles = [article for result in source_results for article in result.get("articles", []) if isinstance(article, dict)]
        articles.extend(article for result in topic_results for article in result.get("articles", []) if isinstance(article, dict))
        failed_items = [item for result in [*source_results, *topic_results] for item in result.get("failed_items", []) if isinstance(item, dict)]
        request_traces = [trace for result in [*source_results, *topic_results] for trace in result.get("request_traces", []) if isinstance(trace, dict)]

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "reddit",
            "articles": articles,
        }
        meta = build_step_meta(
            step_key="reddit",
            status="ok" if ok_calls == total_calls and total_posts > 0 else ("partial" if ok_calls > 0 and total_posts > 0 else "error"),
            elapsed_s=sum(float(result.get("elapsed_s", 0) or 0) for result in [*source_results, *topic_results]),
            items=total_posts,
            calls_total=total_calls,
            calls_ok=ok_calls,
            failed_items=failed_items,
            request_traces=request_traces,
        )
        write_result_with_meta(args.output, output, meta)

        logger.info(
            "✅ Done: %d/%d sources ok, %d/%d query groups ok, %d posts → %s",
            ok_sources,
            len(source_results),
            ok_topics,
            len(topic_results),
            total_posts,
            args.output,
        )
        return 0 if total_posts > 0 else 1
    except Exception as exc:
        logger.error("💥 Reddit fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
