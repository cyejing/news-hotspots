#!/usr/bin/env python3
"""
Fetch topic-grouped Google News results via bb-browser.

Uses topic search queries from topics.json and calls `bb-browser site google/news`
sequentially for each query. Output shape mirrors the former web-search payload,
but uses `source_type=google`.
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
    from config_loader import load_merged_topics
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_topics

COOLDOWN_SECONDS = float(os.environ.get("BB_BROWSER_GOOGLE_COOLDOWN_SECONDS", "12.0"))
DEFAULT_TIMEOUT = 180
DEFAULT_RESULTS_PER_QUERY = 5
MAX_RESULTS_PER_QUERY = 10
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


def run_bb_browser_site(args: Sequence[str], timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    global _last_success_at
    throttle_after_success()
    result = subprocess.run(
        ["bb-browser", "site", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
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
    display = topic.get("display", {})
    max_items = display.get("max_items", DEFAULT_RESULTS_PER_QUERY)
    try:
        return max(1, min(MAX_RESULTS_PER_QUERY, int(max_items)))
    except (TypeError, ValueError):
        return DEFAULT_RESULTS_PER_QUERY


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
    queries = search.get("queries", [])
    exclude = search.get("exclude", [])
    per_query = result_count_for_topic(topic)

    query_stats = []
    dedup_by_url: Dict[str, Dict[str, Any]] = {}

    for query in queries:
        compiled_query = build_google_query(query, exclude)
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
                    "topics": [topic.get("id")],
                    "publisher": normalize_text(item.get("source", "")),
                    "google_query": compiled_query,
                }
                if not article["title"] or not article["link"]:
                    continue
                dedup_by_url.setdefault(article["link"], article)
                kept += 1
            query_stats.append({"query": compiled_query, "status": "ok", "count": kept})
        except Exception as exc:
            logger.warning("Google News query failed [%s]: %s", topic.get("id"), exc)
            query_stats.append({"query": compiled_query, "status": "error", "count": 0, "error": str(exc)[:200]})

    articles = list(dedup_by_url.values())
    articles.sort(key=lambda article: article.get("date", ""), reverse=True)
    ok_queries = sum(1 for stat in query_stats if stat["status"] == "ok")
    return {
        "topic_id": topic.get("id"),
        "status": "ok" if articles else "error",
        "queries_executed": len(queries),
        "queries_ok": ok_queries,
        "query_stats": query_stats,
        "items": len(articles),
        "count": len(articles),
        "articles": articles,
    }


def main() -> int:
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
    args = parser.parse_args()

    logger = setup_logging(args.verbose)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-digest-google-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        topics = load_merged_topics(args.defaults, args.config)
        logger.info("Fetching Google News for %d topics sequentially", len(topics))
        logger.info("Google bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
        topic_results = [fetch_topic(topic, logger) for topic in topics if topic.get("search", {}).get("queries")]
        ok_topics = sum(1 for result in topic_results if result["status"] == "ok")
        total_articles = sum(result.get("count", 0) for result in topic_results)
        total_queries = sum(len(result.get("query_stats", [])) for result in topic_results)
        ok_queries = sum(
            1
            for result in topic_results
            for stat in result.get("query_stats", [])
            if isinstance(stat, dict) and stat.get("status") == "ok"
        )
        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "google",
            "defaults_dir": str(args.defaults),
            "config_dir": str(args.config) if args.config else None,
            "calls_total": total_queries,
            "calls_ok": ok_queries,
            "calls_kind": "queries",
            "items_total": total_articles,
            "queries_total": total_queries,
            "queries_ok": ok_queries,
            "topics_total": len(topic_results),
            "topics_ok": ok_topics,
            "total_articles": total_articles,
            "topics": topic_results,
        }
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(output, handle, ensure_ascii=False, indent=2)
        logger.info("✅ Done: %d/%d topics ok, %d articles → %s", ok_topics, len(topic_results), total_articles, args.output)
        return 0 if ok_topics == len(topic_results) else 1
    except Exception as exc:
        logger.error("💥 Google fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
