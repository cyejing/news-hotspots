#!/usr/bin/env python3
"""
Render merged pipeline output into compact hotspots JSON and Markdown.

Usage:
    python3 merge-hotspots.py --input <merge-sources.json> --archive <archive-root> [--top <n>] [--topic <id>]
"""

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


def get_sorted_articles(source_type_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    articles = source_type_data.get("articles", [])
    if not isinstance(articles, list):
        return []
    return [article for article in articles if isinstance(article, dict)]


def humanize_topic_id(topic_id: str) -> str:
    return topic_id.replace("-", " ").replace("_", " ").title()


def normalize_title_key(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_link_key(value: Any) -> str:
    return str(value or "").strip().lower().rstrip("/")


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
    title = (
        article.get("title")
        or article.get("name")
        or article.get("repo")
        or article.get("source_name")
        or link
        or ""
    )
    return {
        "rank": rank,
        "topic": article.get("topic", ""),
        "title": title,
        "link": link,
        "hotspot_score": round(article.get("final_score", 0), 1),
        "source_type": article.get("source_type", ""),
        "source_name": article.get("source_name", ""),
        "display_name": article.get("display_name"),
        "summary": (article.get("snippet") or article.get("summary") or "").strip(),
        "source_names": article.get("source_names", []),
        "source_name_count": article.get("source_name_count", 1),
        "metrics": normalize_metrics(article),
        "published_at": article.get("date") or article.get("published_at"),
    }


def render_metrics(metrics: Dict[str, Any]) -> str:
    ordered_keys = ["likes", "comments", "replies", "retweets", "score"]
    parts = [f"{key}={metrics[key]}" for key in ordered_keys if key in metrics]
    return ", ".join(parts)


def load_seen_daily_keys(json_dir: Path) -> Tuple[Set[str], Set[str]]:
    seen_titles: Set[str] = set()
    seen_links: Set[str] = set()
    if not json_dir.exists():
        return seen_titles, seen_links

    for file_path in sorted(json_dir.glob("daily*.json")):
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            continue
        for topic in data.get("topics", []):
            for item in topic.get("items", []):
                title_key = normalize_title_key(item.get("title"))
                link_key = normalize_link_key(item.get("link"))
                if title_key:
                    seen_titles.add(title_key)
                if link_key:
                    seen_links.add(link_key)
    return seen_titles, seen_links


def is_seen_article(article: Dict[str, Any], seen_titles: Set[str], seen_links: Set[str]) -> bool:
    title_key = normalize_title_key(article.get("title"))
    link_key = normalize_link_key(article.get("link") or article.get("reddit_url") or article.get("external_url"))
    return (title_key and title_key in seen_titles) or (link_key and link_key in seen_links)


def article_key(article: Dict[str, Any]) -> Tuple[str, str]:
    return (
        normalize_title_key(article.get("title")),
        normalize_link_key(article.get("link") or article.get("reddit_url") or article.get("external_url")),
    )


def score_sort_key(article: Dict[str, Any], source_type_order: Dict[str, int]) -> Tuple[float, int, str]:
    return (
        -float(article.get("final_score", 0) or 0),
        source_type_order.get(str(article.get("source_type", "") or ""), len(source_type_order)),
        str(article.get("title", "") or ""),
    )


def build_topic_candidates(
    data: Dict[str, Any],
    topic_filter: Optional[str],
    seen_titles: Set[str],
    seen_links: Set[str],
) -> Tuple[Dict[str, Dict[str, List[Dict[str, Any]]]], Dict[str, int], Dict[str, int], List[str]]:
    source_types = data.get("source_types", {})
    topic_candidates: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    available_counts: Dict[str, int] = {}
    remaining_counts: Dict[str, int] = {}
    all_articles: List[Dict[str, Any]] = []
    source_type_order = {source_type: index for index, source_type in enumerate(source_types.keys())}

    for source_type, source_type_data in source_types.items():
        sorted_articles = get_sorted_articles(source_type_data)
        for article in sorted_articles:
            topic_id = str(article.get("topic") or "uncategorized")
            if topic_filter and topic_id != topic_filter:
                continue
            available_counts[topic_id] = available_counts.get(topic_id, 0) + 1
            all_articles.append(article)
            if is_seen_article(article, seen_titles, seen_links):
                continue
            remaining_counts[topic_id] = remaining_counts.get(topic_id, 0) + 1
            topic_candidates.setdefault(topic_id, {})
            topic_candidates[topic_id].setdefault(source_type, [])
            topic_candidates[topic_id][source_type].append(article)

    topic_order: List[str] = []
    seen_topics: Set[str] = set()
    for article in sorted(all_articles, key=lambda item: score_sort_key(item, source_type_order)):
        topic_id = str(article.get("topic") or "uncategorized")
        if topic_filter and topic_id != topic_filter:
            continue
        if topic_id in seen_topics:
            continue
        seen_topics.add(topic_id)
        topic_order.append(topic_id)

    for topic_id in available_counts:
        if topic_id not in seen_topics:
            topic_order.append(topic_id)

    return topic_candidates, available_counts, remaining_counts, topic_order


def select_topic_articles(topic_source_candidates: Dict[str, List[Dict[str, Any]]], top_n: int) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    selected_keys: Set[Tuple[str, str]] = set()
    source_types = list(topic_source_candidates.keys())
    offsets = {source_type: 0 for source_type in source_types}

    while len(selected) < top_n:
        progressed = False
        for source_type in source_types:
            source_articles = topic_source_candidates.get(source_type, [])
            offset = offsets[source_type]
            while offset < len(source_articles):
                article = source_articles[offset]
                offset += 1
                key = article_key(article)
                if key in selected_keys:
                    continue
                selected.append(article)
                selected_keys.add(key)
                progressed = True
                break
            offsets[source_type] = offset
            if len(selected) >= top_n:
                break
        if not progressed:
            break

    return selected


def build_markdown(hotspots: Dict[str, Any], mode: str = "daily", extra_sections: str = "") -> str:
    normalized_mode = str(mode or "daily").strip().lower() or "daily"
    lines: List[str] = [f"# {hotspots.get('generated_at', '')[:10] or '<DATE>'} {normalized_mode} 全球科技与 AI 热点"]
    for topic in hotspots.get("topics", []):
        topic_title = topic.get("title") or humanize_topic_id(str(topic.get("id", "")))
        lines.append(f"## {topic_title}")
        for item in topic.get("items", []):
            hotspot_score = item.get("hotspot_score", 0)
            link = item.get("link", "")
            title = item.get("title", "")
            source_name = item.get("source_name") or item.get("display_name") or item.get("source_type", "")
            lines.append(f"{item.get('rank', 0)}. ⭐{hotspot_score:.1f} | [{title}]({link})  ")
            metrics = item.get("metrics", {}) if isinstance(item.get("metrics"), dict) else {}
            if metrics:
                lines.append(f"   来源：{source_name} | 指标：{render_metrics(metrics)}")
            else:
                lines.append(f"   来源：{source_name}")
        lines.append("")
    if extra_sections:
        lines.append(extra_sections.strip())
    return "\n".join(lines).rstrip() + "\n"


def build_hotspots(
    data: Dict[str, Any],
    top_n: int = 15,
    topic_filter: Optional[str] = None,
    seen_titles: Optional[Set[str]] = None,
    seen_links: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    topic_order: List[str] = []
    topic_entries: List[Dict[str, Any]] = []
    source_breakdown: Dict[str, int] = {}
    effective_seen_titles = seen_titles or set()
    effective_seen_links = seen_links or set()
    topic_candidates, available_counts, remaining_counts, ordered_topics = build_topic_candidates(
        data,
        topic_filter,
        effective_seen_titles,
        effective_seen_links,
    )

    for topic_id in ordered_topics:
        limited_articles = select_topic_articles(topic_candidates.get(topic_id, {}), top_n)
        items = [hotspot_item(article, rank=index) for index, article in enumerate(limited_articles, start=1)]

        for item in items:
            source_type = item.get("source_type", "")
            source_breakdown[source_type] = source_breakdown.get(source_type, 0) + 1

        topic_order.append(topic_id)
        topic_entries.append(
            {
                "id": topic_id,
                "title": humanize_topic_id(topic_id),
                "available_article_count": available_counts.get(topic_id, 0),
                "remaining_article_count": remaining_counts.get(topic_id, 0),
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


def build_hotspots_debug(
    data: Dict[str, Any],
    hotspots: Dict[str, Any],
    top_n: int = 15,
    topic_filter: Optional[str] = None,
    seen_titles: Optional[Set[str]] = None,
    seen_links: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    debug_topics: List[Dict[str, Any]] = []
    effective_seen_titles = seen_titles or set()
    effective_seen_links = seen_links or set()
    topic_candidates, available_counts, remaining_counts, ordered_topics = build_topic_candidates(
        data,
        topic_filter,
        effective_seen_titles,
        effective_seen_links,
    )

    for topic_id in ordered_topics:
        topic_source_candidates = topic_candidates.get(topic_id, {})
        limited_articles = select_topic_articles(topic_source_candidates, top_n)
        debug_items: List[Dict[str, Any]] = []
        for rank, article in enumerate(limited_articles, start=1):
            debug_items.append(
                {
                    "rank": rank,
                    "title": article.get("title", ""),
                    "link": article.get("link") or article.get("reddit_url") or article.get("external_url", ""),
                    "hotspot_score": round(article.get("final_score", 0), 1),
                    "source_type": article.get("source_type", ""),
                    "source_name": article.get("source_name", ""),
                    "selection_debug": {
                        "_comment": "热点阶段会先跳过当天已看过的 daily*.json 条目，再按 topic 重建候选池，然后按 source_type 轮转选择每个 source_type 下该 topic 的首条候选并持续补满。",
                        "topic_rank": rank,
                        "selected_for_output": True,
                        "selected_reason_zh": f"该条内容在跳过当天已看过条目后，被热点阶段选入当前批次前 {top_n}。",
                        "final_score_formula": "base_priority_score + fetch_local_rank_score + history_score + cross_source_hot_score + recency_score",
                        "final_score_components": article.get("scoring_debug", {}).get("final_score", {}).get("components", {}),
                        "final_score": article.get("final_score", 0),
                        "hotspot_score": round(article.get("final_score", 0), 1),
                        "hotspot_score_comment_zh": "最终展示给用户的分数，等于 final_score 保留 1 位小数。",
                        "same_day_dedup_applied": True,
                        "source_type_first_pass": True,
                    },
                }
            )

        debug_topics.append(
            {
                "id": topic_id,
                "title": humanize_topic_id(topic_id),
                "available_article_count": available_counts.get(topic_id, 0),
                "remaining_article_count": remaining_counts.get(topic_id, 0),
                "selected_article_count": len(limited_articles),
                "omitted_article_count": max(remaining_counts.get(topic_id, 0) - len(limited_articles), 0),
                "items": debug_items,
            }
        )

    return {
        "generated_at": hotspots.get("generated_at"),
        "total_articles": hotspots.get("total_articles", 0),
        "_comment": "merge-hotspots 调试输出。用于解释热点阶段为什么选中这些条目，以及展示分数如何得到。",
        "scoring_debug": {
            "_comment": "热点阶段沿用 merge-sources 的 final_score 作为展示分数，但会先做当天去重，再按 topic 重建候选池，并按 source_type 轮转填充 top_n。",
            "hotspot_score": "round(upstream_final_score, 1)",
            "hotspot_score_comment_zh": "展示分数 = 上游 final_score 保留 1 位小数。",
            "topic_ordering": "filter_seen_same_day -> rebuild_topic_candidates_from_source_types -> round_robin_first_article_per_source_type",
            "topic_ordering_comment_zh": "先跳过当天已看过条目，再按 topic 重建候选池，然后按 source_type 轮转取每个 source_type 下该 topic 的当前首条候选，直到补满。",
            "top_n_cutoff": top_n,
        },
        "topics": debug_topics,
    }


def resolve_debug_output(debug_dir: Optional[Path]) -> Optional[Path]:
    if not debug_dir:
        return None
    debug_dir.mkdir(parents=True, exist_ok=True)
    return debug_dir / "merge-hotspots.json"


def ensure_archive_dirs(archive_root: Path) -> Tuple[Path, Path, Path]:
    date_dir = archive_root / datetime.now().astimezone().date().isoformat()
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

    _, json_dir, markdown_dir = ensure_archive_dirs(args.archive)
    seen_titles, seen_links = load_seen_daily_keys(json_dir)

    hotspots_json = build_hotspots(
        data,
        top_n=args.top,
        topic_filter=args.topic,
        seen_titles=seen_titles,
        seen_links=seen_links,
    )
    debug_output = resolve_debug_output(args.debug)
    if debug_output:
        debug_payload = build_hotspots_debug(
            data,
            hotspots_json,
            top_n=args.top,
            topic_filter=args.topic,
            seen_titles=seen_titles,
            seen_links=seen_links,
        )
        debug_output.write_text(json.dumps(debug_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    json_output, markdown_output = resolve_archive_pair(json_dir, markdown_dir, stem=args.mode)
    json_output.write_text(json.dumps(hotspots_json, ensure_ascii=False, indent=2), encoding="utf-8")
    merged_archive_output = json_dir / "merge-sources.json"
    if args.input.resolve() != merged_archive_output.resolve():
        shutil.copy2(args.input, merged_archive_output)
    markdown = build_markdown(hotspots_json, mode=args.mode, extra_sections=args.extra_sections)
    markdown_output.write_text(markdown, encoding="utf-8")
    print(f"ARCHIVED_JSON={json_output}")
    print(f"ARCHIVED_MARKDOWN={markdown_output}")
    print(f"ARCHIVED_MERGED_JSON={merged_archive_output}")
    if debug_output:
        print(f"DEBUG_JSON={debug_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
