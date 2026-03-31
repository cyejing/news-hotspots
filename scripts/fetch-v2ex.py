#!/usr/bin/env python3
"""
Fetch V2EX hot topics via bb-browser.

Uses the installed `bb-browser site v2ex/hot` adapter to retrieve structured
hot-topic data, converts it into the news-hotspots source format, and keeps
bb-browser calls serialized with a conservative cooldown after each success.

Usage:
    python3 fetch-v2ex.py --output v2ex.json --verbose
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
    from config_loader import load_merged_topic_rules
    from topic_utils import resolve_primary_topic
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_topic_rules
    from topic_utils import resolve_primary_topic

SOURCE_ID = "v2ex-api"
SOURCE_NAME = "V2EX Hot"
SOURCE_PRIORITY = 4
DEFAULT_TIMEOUT = 60
COOLDOWN_SECONDS = float(os.environ.get("BB_BROWSER_V2EX_COOLDOWN_SECONDS", "5.0"))

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


def run_bb_browser_site(command: Sequence[str], timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    global _last_success_at
    throttle_after_success()

    result = subprocess.run(
        ["bb-browser", "site", *command],
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
    topic_rules: Optional[Dict[str, Any]] = None,
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
    payload = run_bb_browser_site(["v2ex/hot"])
    raw_topics = payload.get("topics", [])
    articles: List[Dict[str, Any]] = []
    effective_defaults_dir = defaults_dir or Path("config/defaults")
    topic_rules = load_merged_topic_rules(effective_defaults_dir, config_dir)

    for item in raw_topics:
        article = transform_topic(item, topic_rules=topic_rules)
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
        "topic": resolve_primary_topic([article.get("topic") for article in articles], rules=topic_rules),
        "status": "ok" if articles else "error",
        "items": len(articles),
        "count": len(articles),
        "fetched_count": len(raw_topics),
        "articles": articles,
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
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-v2ex-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        data = fetch_v2ex_hot(logger, defaults_dir=args.defaults, config_dir=args.config)
        data["defaults_dir"] = str(args.defaults)
        data["config_dir"] = str(args.config) if args.config else None
        data["hours"] = args.hours
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
        logger.info(
            "✅ Done: %d/%d sources ok, %d articles → %s",
            data.get("sources_ok", 0),
            data.get("sources_total", 0),
            data.get("total_articles", 0),
            args.output,
        )
        return 0 if data.get("sources_ok", 0) else 1
    except Exception as exc:
        logger.error("💥 V2EX fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
