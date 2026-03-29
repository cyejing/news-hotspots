#!/usr/bin/env python3
"""
Render merged pipeline output into compact hotspots JSON and Markdown.

Usage:
    python3 merge-hotspots.py --input <merged.json> --archive <archive-root> [--top <n>] [--topic <id>]
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


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


def hotspot_item(article: Dict[str, Any], rank: int) -> Dict[str, Any]:
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


def render_metrics(metrics: Dict[str, Any]) -> str:
    ordered_keys = ["likes", "comments", "replies", "retweets", "score"]
    parts = [f"{key}={metrics[key]}" for key in ordered_keys if key in metrics]
    return ", ".join(parts)


def build_markdown(hotspots: Dict[str, Any], mode: str = "daily", extra_sections: str = "") -> str:
    normalized_mode = str(mode or "daily").strip().lower() or "daily"
    lines: List[str] = [f"# {hotspots.get('generated_at', '')[:10] or '<DATE>'} {normalized_mode} 全球科技与 AI 热点"]
    for topic in hotspots.get("topics", []):
        topic_title = topic.get("title") or humanize_topic_id(str(topic.get("id", "")))
        lines.append(f"## {topic_title}")
        for item in topic.get("items", []):
            score = item.get("score", 0)
            link = item.get("link", "")
            title = item.get("title", "")
            source_name = item.get("source_name") or item.get("display_name") or item.get("source_type", "")
            lines.append(f"{item.get('rank', 0)}. ⭐{score:.1f} | [{title}]({link})  ")
            metrics = item.get("metrics", {}) if isinstance(item.get("metrics"), dict) else {}
            if metrics:
                lines.append(f"   来源：{source_name} | 指标：{render_metrics(metrics)}")
            else:
                lines.append(f"   来源：{source_name}")
        lines.append("")
    if extra_sections:
        lines.append(extra_sections.strip())
    return "\n".join(lines).rstrip() + "\n"


def build_hotspots(data: Dict[str, Any], top_n: int = 15, topic_filter: Optional[str] = None) -> Dict[str, Any]:
    topics = data.get("topics", {})
    topic_order: List[str] = []
    topic_entries: List[Dict[str, Any]] = []
    source_breakdown: Dict[str, int] = {}

    for topic_id, topic_data in topics.items():
        if topic_filter and topic_id != topic_filter:
            continue

        sorted_articles = get_sorted_articles(topic_data)
        limited_articles = sorted_articles[:top_n]
        items = [hotspot_item(article, rank=index) for index, article in enumerate(limited_articles, start=1)]

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


def resolve_debug_output(debug_dir: Optional[Path]) -> Optional[Path]:
    if not debug_dir:
        return None
    debug_dir.mkdir(parents=True, exist_ok=True)
    return debug_dir / "merge-hotspots.json"


def ensure_archive_dirs(archive_root: Path) -> Tuple[Path, Path, Path]:
    date_dir = archive_root / datetime.now(timezone.utc).date().isoformat()
    json_dir = date_dir / "json"
    markdown_dir = date_dir / "markdown"
    meta_dir = date_dir / "meta"
    json_dir.mkdir(parents=True, exist_ok=True)
    markdown_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)
    return date_dir, json_dir, markdown_dir


def resolve_archive_pair(json_dir: Path, markdown_dir: Path, stem: str = "daily") -> Tuple[Path, Path]:
    counter = 0
    while True:
        suffix = "" if counter == 0 else str(counter)
        basename = f"{stem}{suffix}"
        json_path = json_dir / f"{basename}.json"
        markdown_path = markdown_dir / f"{basename}.md"
        if not json_path.exists() and not markdown_path.exists():
            return json_path, markdown_path
        counter += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render merged data into compact hotspots JSON and archived Markdown")
    parser.add_argument("--input", "-i", type=Path, required=True, help="Internal pipeline JSON input")
    parser.add_argument("--archive", dest="archive", type=Path, required=True, help="Archive root dir for final hotspots outputs")
    parser.add_argument("--debug", type=Path, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--top", "-n", type=int, default=5, help="Top N articles per topic")
    parser.add_argument("--topic", "-t", type=str, default=None, help="Filter to specific topic")
    parser.add_argument("--mode", type=str, default="daily", choices=["daily", "weekly"], help="Hotspots mode label and archive file stem")
    parser.add_argument("--extra-sections", type=str, default="", help="Optional Markdown tail section")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.input.exists():
        print(f"Error: {args.input} not found.")
        return 1

    try:
        with open(args.input, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        print(f"Error: failed to read {args.input}: {exc}")
        return 1

    hotspots_json = build_hotspots(data, top_n=args.top, topic_filter=args.topic)
    debug_output = resolve_debug_output(args.debug)
    if debug_output:
        debug_output.write_text(json.dumps(hotspots_json, ensure_ascii=False, indent=2), encoding="utf-8")

    _, json_dir, markdown_dir = ensure_archive_dirs(args.archive)
    json_output, markdown_output = resolve_archive_pair(json_dir, markdown_dir, stem=args.mode)
    json_output.write_text(json.dumps(hotspots_json, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown = build_markdown(hotspots_json, mode=args.mode, extra_sections=args.extra_sections)
    markdown_output.write_text(markdown, encoding="utf-8")
    print(f"ARCHIVED_JSON={json_output}")
    print(f"ARCHIVED_MARKDOWN={markdown_output}")
    if debug_output:
        print(f"DEBUG_JSON={debug_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
