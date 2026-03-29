#!/usr/bin/env python3
"""
Fetch Reddit posts via bb-browser site adapters.

Supports two source modes from sources.json:
- hot mode: subreddit-based sources use `bb-browser site reddit/hot`
- search mode: sources with `query` or `search_query` use `bb-browser site reddit/search`

All Reddit fetches run sequentially. Search mode never uses concurrency.
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
    from config_loader import load_merged_sources
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_sources

COOLDOWN_SECONDS = float(os.environ.get("BB_BROWSER_REDDIT_COOLDOWN_SECONDS", "6.0"))
DEFAULT_TIMEOUT = 180
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


def load_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    all_sources = load_merged_sources(defaults_dir, config_dir)
    return [
        source for source in all_sources
        if source.get("type") == "reddit" and source.get("enabled", True)
    ]


def source_mode(source: Dict[str, Any]) -> str:
    if source.get("query") or source.get("search_query"):
        return "search"
    return "hot"


def hours_to_reddit_time(hours: int) -> str:
    if hours <= 24:
        return "day"
    if hours <= 24 * 7:
        return "week"
    if hours <= 24 * 30:
        return "month"
    if hours <= 24 * 365:
        return "year"
    return "all"


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


def parse_post(item: Dict[str, Any], source: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
    num_comments = int(
        data.get("num_comments", data.get("comments", data.get("comment_count", 0))) or 0
    )
    min_score = int(source.get("min_score", 0) or 0)
    if score < min_score:
        return None

    flair = data.get("link_flair_text") or data.get("flair")
    is_self = bool(data.get("is_self", False))
    summary = (data.get("selftext") or data.get("text") or data.get("snippet") or "").strip()

    return {
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
        "topics": source.get("topics", [])[:],
        "metrics": {
            "score": score,
            "num_comments": num_comments,
            "upvote_ratio": data.get("upvote_ratio"),
        },
    }


def fetch_hot_source(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    subreddit = source.get("subreddit")
    limit = int(source.get("limit", 25) or 25)
    args = ["reddit/hot"]
    if subreddit:
        args.append(subreddit)
    args.append(str(limit))
    payload = run_bb_browser_site(args)
    return extract_posts(payload)


def fetch_search_source(source: Dict[str, Any], hours: int) -> List[Dict[str, Any]]:
    query = source.get("query") or source.get("search_query")
    if not query:
        raise ValueError(f"Reddit search source missing query: {source.get('id', 'unknown')}")

    limit = int(source.get("limit", 25) or 25)
    sort = source.get("sort", "relevance") or "relevance"
    time_filter = source.get("time") or hours_to_reddit_time(hours)

    args = ["reddit/search", str(query)]
    if source.get("subreddit"):
        args.append(str(source["subreddit"]))
    args.extend(["--sort", str(sort), "--time", str(time_filter), str(limit)])
    payload = run_bb_browser_site(args)
    return extract_posts(payload)


def fetch_source(source: Dict[str, Any], hours: int) -> Dict[str, Any]:
    mode = source_mode(source)
    try:
        if mode == "search":
            raw_posts = fetch_search_source(source, hours)
        else:
            raw_posts = fetch_hot_source(source)

        articles = []
        for item in raw_posts:
            article = parse_post(item, source)
            if article:
                articles.append(article)

        return {
            "source_id": source.get("id"),
            "source_type": "reddit",
            "name": source.get("name", source.get("id", "unknown")),
            "subreddit": source.get("subreddit"),
            "query": source.get("query") or source.get("search_query"),
            "sort": source.get("sort", "hot"),
            "mode": mode,
            "priority": source.get("priority", 3),
            "topics": source.get("topics", []),
            "status": "ok",
            "attempts": 1,
            "items": len(articles),
            "count": len(articles),
            "articles": articles,
        }
    except Exception as exc:
        return {
            "source_id": source.get("id"),
            "source_type": "reddit",
            "name": source.get("name", source.get("id", "unknown")),
            "subreddit": source.get("subreddit"),
            "query": source.get("query") or source.get("search_query"),
            "sort": source.get("sort", "hot"),
            "mode": mode,
            "priority": source.get("priority", 3),
            "topics": source.get("topics", []),
            "status": "error",
            "attempts": 1,
            "error": str(exc)[:200],
            "items": 0,
            "count": 0,
            "articles": [],
        }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sequential Reddit fetcher via bb-browser site adapters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-reddit.py --defaults config/defaults --config workspace/config --hours 48 --output reddit.json
    python3 fetch-reddit.py --output reddit.json --verbose
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--hours", type=int, default=48, help="Used for search time mapping")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    logger = setup_logging(args.verbose)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-reddit-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        sources = load_sources(args.defaults, args.config)
        logger.info("Fetching %d Reddit sources sequentially", len(sources))
        logger.info("Reddit bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
        results = []
        for source in sources:
            result = fetch_source(source, args.hours)
            results.append(result)
            if result["status"] == "ok":
                logger.info("✅ %s: %d posts", result["name"], result["count"])
            else:
                logger.warning("❌ %s: %s", result["name"], result.get("error"))

        ok_count = sum(1 for result in results if result["status"] == "ok")
        total_posts = sum(result.get("count", 0) for result in results)
        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "reddit",
            "source": "reddit",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "defaults_dir": str(args.defaults),
            "config_dir": str(args.config) if args.config else None,
            "hours": args.hours,
            "calls_total": len(results),
            "calls_ok": ok_count,
            "items_total": total_posts,
            "sources_total": len(results),
            "sources_ok": ok_count,
            "total_articles": total_posts,
            "sources": results,
            "subreddits_total": len(results),
            "subreddits_ok": ok_count,
            "total_posts": total_posts,
            "subreddits": results,
        }
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(output, handle, ensure_ascii=False, indent=2)

        logger.info("✅ Done: %d/%d sources ok, %d posts → %s", ok_count, len(results), total_posts, args.output)
        return 0 if ok_count == len(results) else 1
    except Exception as exc:
        logger.error("💥 Reddit fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
