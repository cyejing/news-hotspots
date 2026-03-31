#!/usr/bin/env python3
"""
Fetch Zhihu hot topics via bb-browser.

Uses `bb-browser site zhihu/hot` to retrieve structured hot-list
data, converts it into the news-hotspots source format, and keeps bb-browser
calls serialized with a conservative cooldown after each success.
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
from typing import Any, Dict, List, Optional, Sequence

try:
    from config_loader import load_merged_topic_rules
    from fetch_timing import build_request_trace, summarize_request_traces
    from topic_utils import resolve_primary_topic
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_topic_rules
    from fetch_timing import build_request_trace, summarize_request_traces
    from topic_utils import resolve_primary_topic

SOURCE_ID = "zhihu-hot"
SOURCE_NAME = "Zhihu Hot"
SOURCE_PRIORITY = 3
DEFAULT_TIMEOUT = 120
DEFAULT_LIMIT = 20
COOLDOWN_SECONDS = float(os.environ.get("BB_BROWSER_ZHIHU_COOLDOWN_SECONDS", "6.0"))

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


def clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def truncate_summary(value: Any, limit: int = 240) -> str:
    text = clean_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def first_non_empty(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def parse_number(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    text = clean_text(value).lower().replace(",", "")
    if not text:
        return None

    match = re.search(r"(\d+(?:\.\d+)?)\s*([万亿w]?)", text)
    if not match:
        return None

    number = float(match.group(1))
    unit = match.group(2)
    if unit in {"万", "w"}:
        number *= 10000
    elif unit == "亿":
        number *= 100000000
    return int(number)


def extract_hot_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    for key in ("items", "questions", "data", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            for nested_key in ("items", "questions", "data", "list"):
                nested = value.get(nested_key)
                if isinstance(nested, list):
                    return [item for item in nested if isinstance(item, dict)]
    return []


def transform_hot_item(item: Dict[str, Any], topic_rules: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    target = item.get("target") if isinstance(item.get("target"), dict) else {}
    title = first_non_empty(
        item.get("title"),
        item.get("question_title"),
        target.get("title"),
        target.get("name"),
    )
    if not title:
        return None

    question_id = first_non_empty(
        item.get("question_id"),
        item.get("questionId"),
        target.get("id"),
        item.get("id"),
    )
    link = first_non_empty(
        item.get("url"),
        item.get("link"),
        item.get("share_url"),
        item.get("question_url"),
        target.get("url"),
        target.get("share_url"),
    )
    if not link and question_id.isdigit():
        link = f"https://www.zhihu.com/question/{question_id}"
    if not link:
        return None

    summary = truncate_summary(
        item.get("excerpt")
        or item.get("detail_text")
        or item.get("description")
        or target.get("excerpt")
        or target.get("detail_text")
        or target.get("answer_text")
    )
    topic = "social"

    metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
    hot_score = parse_number(
        item.get("hot")
        or item.get("hot_score")
        or metrics.get("hot_score")
        or item.get("hot_text")
    )
    answer_count = parse_number(
        item.get("answer_count")
        or target.get("answer_count")
        or metrics.get("answer_count")
    )
    follower_count = parse_number(
        item.get("follower_count")
        or target.get("follower_count")
        or metrics.get("follower_count")
    )

    return {
        "title": title,
        "link": link,
        "date": "",
        "summary": summary,
        "topic": topic,
        "question_id": question_id,
        "hot_score": hot_score or 0,
        "answer_count": answer_count or 0,
        "follower_count": follower_count or 0,
        "metrics": {
            "hot_score": hot_score,
            "answer_count": answer_count,
            "follower_count": follower_count,
        },
    }


def fetch_zhihu_hot(
    logger: logging.Logger,
    defaults_dir: Optional[Path] = None,
    config_dir: Optional[Path] = None,
    limit: int = DEFAULT_LIMIT,
) -> Dict[str, Any]:
    logger.info("Zhihu bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
    started_at = time.monotonic()
    payload = run_bb_browser_site(["zhihu/hot", str(limit)])
    elapsed_s = time.monotonic() - started_at
    request_trace = build_request_trace("zhihu/hot", elapsed_s, status="ok", backend="bb-browser", adapter="zhihu/hot")
    raw_items = extract_hot_items(payload)
    effective_defaults_dir = defaults_dir or Path("config/defaults")
    topic_rules = load_merged_topic_rules(effective_defaults_dir, config_dir)

    articles: List[Dict[str, Any]] = []
    for item in raw_items:
        article = transform_hot_item(item, topic_rules=topic_rules)
        if article:
            articles.append(article)

    logger.info("Fetched Zhihu hot topics: %d raw, %d kept", len(raw_items), len(articles))

    source_result = {
        "source_id": SOURCE_ID,
        "source_type": "zhihu",
        "name": SOURCE_NAME,
        "priority": SOURCE_PRIORITY,
        "topic": resolve_primary_topic([article.get("topic") for article in articles], rules=topic_rules),
        "status": "ok" if articles else "error",
        "elapsed_s": round(elapsed_s, 3),
        "timing_keywords": request_trace["timing_keywords"],
        "items": len(articles),
        "count": len(articles),
        "fetched_count": len(raw_items),
        "articles": articles,
        "request_timings": [request_trace],
        "request_timing_summary": summarize_request_traces([request_trace]),
    }
    if not articles:
        source_result["error"] = "No tech-relevant Zhihu hot topics found"

    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "source_type": "zhihu",
        "calls_total": 1,
        "calls_ok": 1 if articles else 0,
        "items_total": len(articles),
        "sources_total": 1,
        "sources_ok": 1 if articles else 0,
        "total_articles": len(articles),
        "request_timing_summary": summarize_request_traces([request_trace]),
        "sources": [source_result],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Zhihu hot topics via bb-browser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-zhihu.py --defaults config/defaults --config workspace/config --output zhihu.json
    python3 fetch-zhihu.py --verbose
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Accepted for CLI consistency; not used by Zhihu fetch")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of Zhihu hot items to request (default: 20)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    logger = setup_logging(args.verbose)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-zhihu-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        data = fetch_zhihu_hot(
            logger,
            defaults_dir=args.defaults,
            config_dir=args.config,
            limit=max(1, min(int(args.limit), 50)),
        )
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
        logger.error("💥 Zhihu fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
