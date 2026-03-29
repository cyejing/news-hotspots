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

SOURCE_ID = "v2ex-api"
SOURCE_NAME = "V2EX Hot"
SOURCE_PRIORITY = 4
DEFAULT_TIMEOUT = 90
COOLDOWN_SECONDS = float(os.environ.get("BB_BROWSER_V2EX_COOLDOWN_SECONDS", "5.0"))

_last_success_at: Optional[float] = None

NODE_TOPIC_MAP = {
    "programmer": {"developer-tools"},
    "nodejs": {"developer-tools"},
    "python": {"developer-tools"},
    "java": {"developer-tools"},
    "go": {"developer-tools"},
    "rust": {"developer-tools"},
    "career": {"developer-tools"},
    "devops": {"developer-tools"},
    "linux": {"developer-tools"},
    "database": {"developer-tools"},
    "server": {"developer-tools"},
    "ev": {"technology"},
    "car": {"technology"},
    "apple": {"technology"},
    "android": {"technology"},
    "iphone": {"technology"},
    "ipad": {"technology"},
    "macos": {"technology"},
    "hardware": {"technology"},
    "share": {"developer-tools"},
}

TOPIC_KEYWORDS = {
    "ai-models": [
        "llm", "gpt", "claude", "gemini", "anthropic", "openai", "deepseek",
        "qwen", "kimi", "大模型", "语言模型", "模型", "prompt",
    ],
    "ai-agents": [
        "agent", "agents", "智能体", "copilot", "mcp", "automation", "agentic",
    ],
    "ai-ecosystem": [
        "ai chip", "gpu", "nvidia", "tesla", "spacex", "robotics", "humanoid",
        "芯片", "算力", "存储", "机器人", "特斯拉", "自动驾驶",
    ],
    "technology": [
        "robot", "机器人", "gpu", "芯片", "quantum", "量子", "space", "航天",
        "自动驾驶", "智驾", "iphone", "ipad", "macbook", "mac", "ios",
        "android", "tesla", "logitech", "罗技", "手机", "显卡", "耳机",
        "汽车", "电车",
    ],
    "developer-tools": [
        "python", "node", "nodejs", "nestjs", "fastify", "hono", "javascript",
        "typescript", "backend", "frontend", "api", "docker", "k8s",
        "编程", "程序员", "开发", "后端", "前端", "数据库", "运维",
    ],
    "markets-business": [
        "finance", "stock", "stocks", "market", "fund", "房贷", "股票", "基金",
        "理财", "财务", "经济",
    ],
    "macro-policy": [
        "inflation", "rate cut", "rate hike", "cpi", "央行", "利率", "宏观",
        "监管", "政策", "财政",
    ],
    "cybersecurity": [
        "security", "cve", "漏洞", "隐私", "渗透", "攻击", "数据泄露",
        "hacking", "breach",
    ],
    "world-affairs": [
        "war", "conflict", "sanction", "diplomacy", "election", "government",
        "国际", "外交", "冲突", "政治",
    ],
}


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


def infer_topics(title: str, content: str, node_slug: str, node_name: str) -> List[str]:
    haystack = f"{title}\n{content}\n{node_slug}\n{node_name}".lower()
    topics: Set[str] = set(NODE_TOPIC_MAP.get(node_slug, set()))

    for topic_id, keywords in TOPIC_KEYWORDS.items():
        if any(keyword.lower() in haystack for keyword in keywords):
            topics.add(topic_id)

    ordered = [
        "ai-models",
        "ai-agents",
        "ai-ecosystem",
        "technology",
        "developer-tools",
        "markets-business",
        "macro-policy",
        "world-affairs",
        "cybersecurity",
    ]
    return [topic_id for topic_id in ordered if topic_id in topics]


def transform_topic(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = clean_text(item.get("title", ""))
    link = item.get("url", "")
    if not title or not link:
        return None

    content = item.get("content", "") or ""
    node_slug = (item.get("nodeSlug") or "").strip().lower()
    node_name = clean_text(item.get("node", ""))
    topics = infer_topics(title, content, node_slug, node_name)
    if not topics:
        return None

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
        "topics": topics,
        "replies": replies,
        "author": author,
        "node": node_name,
        "node_slug": node_slug,
        "v2ex_id": item.get("id"),
    }


def fetch_v2ex_hot(logger: logging.Logger) -> Dict[str, Any]:
    logger.info("V2EX bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
    payload = run_bb_browser_site(["v2ex/hot"])
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
        "topics": sorted({topic for article in articles for topic in article.get("topics", [])}),
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
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Accepted for CLI consistency; not used by V2EX fetch")
    parser.add_argument("--config", type=Path, help="Accepted for CLI consistency; not used by V2EX fetch")
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
        data = fetch_v2ex_hot(logger)
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
