#!/usr/bin/env python3
"""
热点产物生成脚本。

职责：
- 读取 `merge-sources.json`
- 以 `topic` 为维度重建候选池
- 执行 same-day 去重与 source_type 轮转选取
- 生成最终归档 JSON / Markdown

执行逻辑：
1. 读取 `merge-sources.json` 中已打分的 article
2. 过滤当天已出现过的内容
3. 以 topic 为单位做 source_type 轮转选取 top N
4. 输出归档 `daily*.json`、`daily*.md`

输出文件职责：
- `archive/<DATE>/json/daily*.json`
  最终用户交付 JSON，同时保留评分与选取 debug 字段
- `archive/<DATE>/markdown/daily*.md`
  最终用户交付 Markdown
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from config_loader import load_merged_runtime_config, load_merged_topics
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config, load_merged_topics

DEFAULT_TOP_N = 5
SCORE_FORMULA = "base_priority_score + fetch_local_rank_score + history_score + cross_source_hot_score + recency_score"
SCORE_FORMULA_ZH = "基础优先级分 + 源内排序分 + 历史重复修正分 + 跨源共振分 + 时效性分"
SCORE_COMPONENTS_ZH = {
    "base_priority_score": "source_priority 转换后的基础分",
    "fetch_local_rank_score": "该内容在所属抓取源内部的排序分",
    "history_score": "与历史热点重复或相似时的修正分",
    "cross_source_hot_score": "被多个 source_type 命中时的加分",
    "recency_score": "按发布时间得到的时效性分",
    "local_extra_score": "抓取源内部热度信号的附加参考分",
}
SELECTION_EXPLANATIONS_ZH = {
    "source_type_rank": "这条内容在所属 source_type 候选列表中的排名",
    "source_type_total_candidates": "该 source_type 本次一共有多少候选内容",
    "selected_after_same_day_dedup": "是否在去掉今天已看过内容之后仍保留并被选中",
    "selected_by_round_robin": "是否通过 source_type 轮转选取逻辑进入最终结果",
    "selection_order": "这条内容在 source_type 轮转选取阶段被选中的先后顺序",
    "display_rank": "这条内容在最终 JSON / Markdown 中按热点分展示的名次",
}


def resolve_config_dir(config_dir: Optional[Path]) -> Optional[Path]:
    if config_dir is None:
        return None
    return config_dir if config_dir.exists() else None


def load_runtime_config(defaults_dir: Path, config_dir: Optional[Path]) -> Dict[str, Any]:
    return load_merged_runtime_config(defaults_dir, resolve_config_dir(config_dir))


def load_topic_metadata(defaults_dir: Path, config_dir: Optional[Path]) -> Dict[str, Dict[str, str]]:
    topics = load_merged_topics(defaults_dir, resolve_config_dir(config_dir))
    metadata: Dict[str, Dict[str, str]] = {}
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        topic_id = str(topic.get("id") or "").strip()
        if not topic_id:
            continue
        metadata[topic_id] = {
            "emoji": str(topic.get("emoji") or "").strip(),
            "label": str(topic.get("label") or "").strip(),
        }
    return metadata


def get_sorted_articles(source_type_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    articles = source_type_data.get("articles", [])
    if not isinstance(articles, list):
        return []
    return [article for article in articles if isinstance(article, dict)]


def humanize_topic_id(topic_id: str) -> str:
    return topic_id.replace("-", " ").replace("_", " ").title()


def topic_display_title(topic_id: str, topic_metadata: Optional[Dict[str, Dict[str, str]]] = None) -> str:
    metadata = topic_metadata.get(topic_id, {}) if isinstance(topic_metadata, dict) else {}
    emoji = str(metadata.get("emoji") or "").strip()
    label = str(metadata.get("label") or "").strip()
    if label:
        localized = label.split("/", 1)[1].strip() if "/" in label else label
        return f"{emoji} {localized}".strip() if emoji else localized
    fallback = humanize_topic_id(topic_id)
    return f"{emoji} {fallback}".strip() if emoji else fallback


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


def build_hotspot_item(
    article: Dict[str, Any],
    rank: int,
    source_rank_index: Dict[Tuple[str, str], Dict[str, int]],
    selection_order: int,
) -> Dict[str, Any]:
    link = article.get("link") or article.get("reddit_url") or article.get("external_url", "")
    title = (
        article.get("title")
        or article.get("name")
        or article.get("repo")
        or link
        or ""
    )
    item = {
        "rank": rank,
        "topic": article.get("topic", ""),
        "title": title,
        "link": link,
        "hotspot_score": round(article.get("final_score", 0), 1),
        "source_type": article.get("source_type", ""),
        "source_name": article.get("source_name", ""),
        "summary": (article.get("snippet") or article.get("summary") or "").strip(),
        "metrics": normalize_metrics(article),
        "published_at": article.get("date") or article.get("published_at"),
    }
    raw_score_components = article.get("score_components", {}) if isinstance(article.get("score_components"), dict) else {}
    item["score_debug"] = {
        "final_score": float(article.get("final_score", 0) or 0),
        "hotspot_score": item["hotspot_score"],
        "formula": SCORE_FORMULA,
        "formula_zh": SCORE_FORMULA_ZH,
        "components": {
            "base_priority_score": float(raw_score_components.get("base_priority_score", 0) or 0),
            "fetch_local_rank_score": float(raw_score_components.get("fetch_local_rank_score", 0) or 0),
            "history_score": float(raw_score_components.get("history_score", 0) or 0),
            "cross_source_hot_score": float(raw_score_components.get("cross_source_hot_score", 0) or 0),
            "recency_score": float(raw_score_components.get("recency_score", 0) or 0),
            "local_extra_score": float(raw_score_components.get("local_extra_score", 0) or 0),
        },
        "components_zh": SCORE_COMPONENTS_ZH,
    }
    item["selection_debug"] = {
        **source_rank_index.get(article_key(article), {"source_type_rank": 0, "source_type_total_candidates": 0}),
        "selected_after_same_day_dedup": True,
        "selected_by_round_robin": True,
        "selection_order": selection_order,
        "display_rank": rank,
        "explanations_zh": SELECTION_EXPLANATIONS_ZH,
    }
    return item


def build_source_rank_index(data: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, int]]:
    rank_index: Dict[Tuple[str, str], Dict[str, int]] = {}
    for source_type, source_type_data in data.get("source_types", {}).items():
        articles = get_sorted_articles(source_type_data)
        total = len(articles)
        for rank, article in enumerate(articles, start=1):
            rank_index[article_key(article)] = {
                "source_type_rank": rank,
                "source_type_total_candidates": total,
            }
    return rank_index


def render_metrics(metrics: Dict[str, Any]) -> str:
    ordered_keys = ["likes", "comments", "replies", "retweets", "score"]
    parts = [f"{key}={metrics[key]}" for key in ordered_keys if key in metrics]
    return ", ".join(parts)


def render_source_label(source_type: str, source_name: str) -> str:
    left = str(source_type or "").strip()
    right = str(source_name or "").strip()
    if left and right:
        return f"{left} - {right}"
    return left or right


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


def build_selection_order_index(selected_articles: List[Dict[str, Any]]) -> Dict[Tuple[str, str], int]:
    return {
        article_key(article): index
        for index, article in enumerate(selected_articles, start=1)
    }


def sort_selected_topic_articles(
    selected_articles: List[Dict[str, Any]],
    topic_source_candidates: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    source_type_order = {
        source_type: index
        for index, source_type in enumerate(topic_source_candidates.keys())
    }
    return sorted(selected_articles, key=lambda article: score_sort_key(article, source_type_order))


def build_markdown(hotspots: Dict[str, Any], mode: str = "daily", extra_sections: str = "") -> str:
    normalized_mode = str(mode or "daily").strip().lower() or "daily"
    summary_parts = [
        f"mode:{normalized_mode}",
        f"total_articles:{hotspots.get('total_articles', 0)}",
    ]
    for source_type, count in sorted((hotspots.get("source_type_counts") or {}).items()):
        summary_parts.append(f"{source_type}:{count}")
    summary_parts.append(f"generated_at:{hotspots.get('generated_at', '')}")
    lines: List[str] = [
        "---",
        f"summary: {' | '.join(summary_parts)}",
        "---",
        f"# {hotspots.get('generated_at', '')[:10] or '<DATE>'} {normalized_mode} 全球科技与 AI 热点",
        "",
    ]
    for topic in hotspots.get("topics", []):
        topic_title = topic.get("title") or humanize_topic_id(str(topic.get("id", "")))
        lines.append(f"## {topic_title}")
        for item in topic.get("items", []):
            hotspot_score = item.get("hotspot_score", 0)
            title = item.get("title", "")
            summary = item.get("summary", "")
            source_type = item.get("source_type", "")
            source_name = item.get("source_name", "")
            metrics = item.get("metrics", {}) if isinstance(item.get("metrics"), dict) else {}
            title_part = f"{title} - {summary}" if summary else title
            source_label = render_source_label(source_type, source_name)
            metrics_part = render_metrics(metrics)
            if metrics_part:
                lines.append(
                    f"{item.get('rank', 0)}. ⭐{hotspot_score:.1f} | {title_part} | *{source_label}* | *{metrics_part}*"
                )
            else:
                lines.append(f"{item.get('rank', 0)}. ⭐{hotspot_score:.1f} | {title_part} | *{source_label}*")
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
    topic_metadata: Optional[Dict[str, Dict[str, str]]] = None,
) -> Dict[str, Any]:
    topic_entries: List[Dict[str, Any]] = []
    candidate_source_breakdown: Dict[str, int] = {
        source_type: int(source_type_data.get("count", 0) or 0)
        for source_type, source_type_data in data.get("source_types", {}).items()
        if isinstance(source_type_data, dict)
    }
    selected_source_breakdown: Dict[str, int] = {}
    displayed_total = 0
    effective_seen_titles = seen_titles or set()
    effective_seen_links = seen_links or set()
    source_rank_index = build_source_rank_index(data)
    topic_candidates, available_counts, remaining_counts, ordered_topics = build_topic_candidates(
        data,
        topic_filter,
        effective_seen_titles,
        effective_seen_links,
    )

    for topic_id in ordered_topics:
        topic_source_candidates = topic_candidates.get(topic_id, {})
        selected_articles = select_topic_articles(topic_source_candidates, top_n)
        selection_order_index = build_selection_order_index(selected_articles)
        display_articles = sort_selected_topic_articles(selected_articles, topic_source_candidates)
        items = [
            build_hotspot_item(
                article,
                rank=index,
                source_rank_index=source_rank_index,
                selection_order=selection_order_index.get(article_key(article), index),
            )
            for index, article in enumerate(display_articles, start=1)
        ]
        displayed_total += len(items)
        for item in items:
            source_type = str(item.get("source_type") or "").strip()
            if not source_type:
                continue
            selected_source_breakdown[source_type] = selected_source_breakdown.get(source_type, 0) + 1

        topic_entries.append(
            {
                "id": topic_id,
                "title": topic_display_title(topic_id, topic_metadata=topic_metadata),
                "available_article_count": available_counts.get(topic_id, 0),
                "remaining_article_count": remaining_counts.get(topic_id, 0),
                "items": items,
            }
        )

    return {
        "generated_at": data.get("generated"),
        "total_articles": displayed_total,
        "source_type_counts": selected_source_breakdown,
        "candidate_total_articles": data.get("output_stats", {}).get("total_articles", 0),
        "candidate_source_type_counts": candidate_source_breakdown,
        "topics": topic_entries,
    }


def ensure_archive_dirs(archive_root: Path) -> Tuple[Path, Path]:
    date_dir = archive_root / datetime.now().astimezone().date().isoformat()
    json_dir = date_dir / "json"
    markdown_dir = date_dir / "markdown"
    json_dir.mkdir(parents=True, exist_ok=True)
    markdown_dir.mkdir(parents=True, exist_ok=True)
    return json_dir, markdown_dir


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
    parser.add_argument("--defaults", type=Path, required=True, help="Defaults config directory")
    parser.add_argument("--config", type=Path, default=None, help="Workspace overlay config directory")
    parser.add_argument("--input", "-i", type=Path, required=True, help="Internal pipeline JSON input")
    parser.add_argument("--archive", dest="archive", type=Path, required=True, help="Archive root dir for final hotspots outputs")
    parser.add_argument("--top", "-n", type=int, default=None, help="Top N articles per topic")
    parser.add_argument("--topic", "-t", type=str, default=None, help="Filter to specific topic")
    parser.add_argument("--mode", type=str, default="daily", choices=["daily", "weekly"], help="Hotspots mode label and archive file stem")
    parser.add_argument("--extra-sections", type=str, default="", help="Optional Markdown tail section")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_config = load_runtime_config(args.defaults, args.config)
    pipeline_config = runtime_config.get("pipeline", {})
    effective_top_n = args.top if args.top is not None else int(pipeline_config.get("default_hotspots_top_n", DEFAULT_TOP_N) or DEFAULT_TOP_N)

    if not args.input.exists():
        print(f"Error: {args.input} not found.")
        return 1

    try:
        with open(args.input, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        print(f"Error: failed to read {args.input}: {exc}")
        return 1

    json_dir, markdown_dir = ensure_archive_dirs(args.archive)
    seen_titles, seen_links = load_seen_daily_keys(json_dir)
    topic_metadata = load_topic_metadata(args.defaults, args.config)

    hotspots_json = build_hotspots(
        data,
        top_n=effective_top_n,
        topic_filter=args.topic,
        seen_titles=seen_titles,
        seen_links=seen_links,
        topic_metadata=topic_metadata,
    )
    json_output, markdown_output = resolve_archive_pair(json_dir, markdown_dir, stem=args.mode)
    json_output.write_text(json.dumps(hotspots_json, ensure_ascii=False, indent=2), encoding="utf-8")
    merged_archive_output = json_dir / "merge-sources.json"
    if args.input.resolve() != merged_archive_output.resolve():
        merged_archive_output.write_text(args.input.read_text(encoding="utf-8"), encoding="utf-8")
    markdown = build_markdown(hotspots_json, mode=args.mode, extra_sections=args.extra_sections)
    markdown_output.write_text(markdown, encoding="utf-8")
    print(f"ARCHIVED_JSON={json_output}")
    print(f"ARCHIVED_MARKDOWN={markdown_output}")
    print(f"ARCHIVED_MERGED_JSON={merged_archive_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
