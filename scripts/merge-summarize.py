#!/usr/bin/env python3
"""
Render merged pipeline output into a compact summary JSON.

Usage:
    python3 merge-summarize.py --input <merged.json> --output <summary.json> [--top <n>] [--topic <id>]
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_sorted_articles(topic_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    articles = topic_data.get("articles", [])
    if not isinstance(articles, list):
        return []
    return sorted(
        [article for article in articles if isinstance(article, dict)],
        key=lambda article: article.get("final_score", article.get("quality_score", 0)),
        reverse=True,
    )


def humanize_topic_id(topic_id: str) -> str:
    return topic_id.replace("-", " ").replace("_", " ").title()


def normalize_metrics(article: Dict[str, Any]) -> Dict[str, Any]:
    raw_metrics = article.get("metrics", {}) if isinstance(article.get("metrics"), dict) else {}
    normalized = {
        "likes": raw_metrics.get("like_count"),
        "retweets": raw_metrics.get("retweet_count"),
        "replies": raw_metrics.get("reply_count", article.get("replies")),
        "comments": article.get("num_comments"),
        "score": article.get("score"),
    }
    return {key: value for key, value in normalized.items() if value not in (None, 0, "", [])}


def summarize_item(article: Dict[str, Any], rank: int) -> Dict[str, Any]:
    link = article.get("link") or article.get("reddit_url") or article.get("external_url", "")
    return {
        "rank": rank,
        "title": article.get("title", ""),
        "link": link,
        "score": round(article.get("final_score", article.get("quality_score", 0)), 1),
        "source_type": article.get("source_type", ""),
        "source_name": article.get("source_name", ""),
        "display_name": article.get("display_name"),
        "summary": (article.get("snippet") or article.get("summary") or "").strip(),
        "all_sources": article.get("all_sources", []),
        "source_count": article.get("source_count", 1),
        "metrics": normalize_metrics(article),
        "published_at": article.get("date") or article.get("published_at"),
    }


def build_summary(data: Dict[str, Any], top_n: int = 15, topic_filter: Optional[str] = None) -> Dict[str, Any]:
    topics = data.get("topics", {})
    topic_order: List[str] = []
    topic_entries: List[Dict[str, Any]] = []
    source_breakdown: Dict[str, int] = {}

    for topic_id, topic_data in topics.items():
        if topic_filter and topic_id != topic_filter:
            continue

        sorted_articles = get_sorted_articles(topic_data)
        limited_articles = sorted_articles[:top_n]
        items = [summarize_item(article, rank=index) for index, article in enumerate(limited_articles, start=1)]

        for item in items:
            source_type = item.get("source_type", "")
            source_breakdown[source_type] = source_breakdown.get(source_type, 0) + 1

        topic_order.append(topic_id)
        topic_entries.append(
            {
                "id": topic_id,
                "title": humanize_topic_id(topic_id),
                "article_count": len(sorted_articles),
                "items": items,
            }
        )

    return {
        "generated_at": data.get("generated"),
        "total_articles": data.get("output_stats", {}).get("total_articles", 0),
        "topic_order": topic_order,
        "source_breakdown": source_breakdown,
        "topics": topic_entries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize merged data into compact JSON")
    parser.add_argument("--input", "-i", type=Path, required=True, help="Internal pipeline JSON input")
    parser.add_argument("--output", "-o", type=Path, required=True, help="Summary JSON output path")
    parser.add_argument("--top", "-n", type=int, default=5, help="Top N articles per topic")
    parser.add_argument("--topic", "-t", type=str, default=None, help="Filter to specific topic")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: {args.input} not found.")
        return 1

    try:
        with open(args.input, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        print(f"Error: failed to read {args.input}: {exc}")
        return 1

    summary_json = build_summary(data, top_n=args.top, topic_filter=args.topic)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary_json, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
