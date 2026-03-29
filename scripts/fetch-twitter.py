#!/usr/bin/env python3
"""
Fetch Twitter/X timelines via bb-browser site adapters.

Reads Twitter sources from sources.json and fetches each configured handle
sequentially using `bb-browser site twitter/tweets`.
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
    from config_loader import load_merged_sources
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_sources

COOLDOWN_SECONDS = float(os.environ.get("BB_BROWSER_TWITTER_COOLDOWN_SECONDS", "7.0"))
DEFAULT_TIMEOUT = 180
DEFAULT_COUNT = 20
MAX_COUNT = 100
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
        if source.get("type") == "twitter" and source.get("enabled", True)
    ]


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
    return max(1, min(MAX_COUNT, count))


def fetch_timeline(source: Dict[str, Any]) -> Dict[str, Any]:
    handle = source.get("handle")
    if not handle:
        raise ValueError(f"Twitter source missing handle: {source.get('id', 'unknown')}")
    count = timeline_count_for_source(source)
    return run_bb_browser_site(["twitter/tweets", handle, str(count)])


def parse_tweet(item: Dict[str, Any], source: Dict[str, Any], cutoff: datetime) -> Optional[Dict[str, Any]]:
    text = truncate_text(item.get("text", ""))
    link = item.get("url", "")
    if not text or not link:
        return None

    tweet_dt = parse_twitter_datetime(item.get("created_at", ""))
    if not within_hours(tweet_dt, cutoff):
        return None

    likes = int(item.get("likes", 0) or 0)
    retweets = int(item.get("retweets", 0) or 0)
    replies = int(item.get("replies", item.get("reply_count", 0)) or 0)
    quotes = int(item.get("quotes", item.get("quote_count", 0)) or 0)
    impressions = item.get("impressions", item.get("impression_count"))

    return {
        "title": text,
        "link": link,
        "date": tweet_dt.isoformat(),
        "topics": source.get("topics", [])[:],
        "metrics": {
            "like_count": likes,
            "retweet_count": retweets,
            "reply_count": replies,
            "quote_count": quotes,
            "impression_count": impressions,
        },
        "tweet_id": item.get("id"),
        "tweet_type": item.get("type"),
        "author": item.get("author") or source.get("handle"),
        "rt_author": item.get("rt_author"),
    }


def fetch_source(source: Dict[str, Any], cutoff: datetime) -> Dict[str, Any]:
    try:
        payload = fetch_timeline(source)
        articles: List[Dict[str, Any]] = []
        for item in payload.get("tweets", []):
            article = parse_tweet(item, source, cutoff)
            if article:
                articles.append(article)

        return {
            "source_id": source.get("id"),
            "source_type": "twitter",
            "name": source.get("name", source.get("id", "unknown")),
            "handle": source.get("handle"),
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
            "source_type": "twitter",
            "name": source.get("name", source.get("id", "unknown")),
            "handle": source.get("handle"),
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
        description="Sequential Twitter/X fetcher via bb-browser site adapters.",
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
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-twitter-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        sources = load_sources(args.defaults, args.config)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
        logger.info("Fetching %d Twitter sources sequentially", len(sources))
        logger.info("Twitter bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)

        results = []
        for source in sources:
            result = fetch_source(source, cutoff)
            results.append(result)
            if result["status"] == "ok":
                logger.info("✅ %s: %d tweets", result["name"], result["count"])
            else:
                logger.warning("❌ %s: %s", result["name"], result.get("error"))

        ok_count = sum(1 for result in results if result["status"] == "ok")
        total_articles = sum(result.get("count", 0) for result in results)
        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "twitter",
            "backend": "bb-browser",
            "defaults_dir": str(args.defaults),
            "config_dir": str(args.config) if args.config else None,
            "hours": args.hours,
            "calls_total": len(results),
            "calls_ok": ok_count,
            "items_total": total_articles,
            "sources_total": len(results),
            "sources_ok": ok_count,
            "total_articles": total_articles,
            "sources": results,
        }

        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(output, handle, ensure_ascii=False, indent=2)

        logger.info("✅ Done: %d/%d sources ok, %d tweets → %s", ok_count, len(results), total_articles, args.output)
        return 0 if ok_count == len(results) else 1
    except Exception as exc:
        logger.error("💥 Twitter fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
