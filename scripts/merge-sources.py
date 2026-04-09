#!/usr/bin/env python3
"""
merge 中间结果生成脚本。

职责：
- 读取各 fetch step 的标准化结果 JSON
- 合并 article 流
- 计算 `final_score`
- 执行历史相似性处理与去重
- 按 `source_type` 输出极简中间结果

执行逻辑：
1. 读取各 fetch 的 `<step>.json`
2. 标准化缺省字段并收集为统一 article 列表
3. 计算各评分分项与 `final_score`
4. 进行相似性聚类与去重
5. 按 `source_type` 分组后输出 `merge-sources.json`

输出文件职责：
- `debug_dir/merge-sources.json`
  merge 阶段唯一中间结果，只给 `merge-hotspots.py` 消费
  保留 `final_score` 与最小 `score_components`，不再承载旧 debug 大对象
"""

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

try:
    from step_registry import ALL_SOURCE_STEPS, STEP_KEYS
    from rapidfuzz import fuzz
    from step_contract import local_now, local_tzinfo, now_iso, to_local_datetime
except ImportError:  # pragma: no cover - defensive fallback
    fuzz = None
    sys.path.append(str(Path(__file__).parent))
    from step_registry import ALL_SOURCE_STEPS, STEP_KEYS
    from step_contract import local_now, local_tzinfo, now_iso, to_local_datetime


SCORING_CONFIG = {
    "fetch_rank_max": 3.0,
    "history_threshold": 0.88,
    "history_scores": [
        (0.96, -16.0),
        (0.92, -12.0),
        (0.88, -8.0),
    ],
    "cross_source_hot_threshold": 0.86,
    "duplicate_threshold": 0.92,
    "cross_source_hot_score_per_extra_type": 2.0,
    "cross_source_hot_score_cap": 6.0,
    "recency_24h_score": 1.0,
    "recency_6h_score": 0.5,
}

SCORE_DEBUG_COMMENTS = {
    "scoring_debug": "评分计算说明，只保留 final_score 公式和各项分值。",
    "final_score": "merge-sources 阶段最终分，由多个分数组件相加得到。",
    "similarity_debug": "相似度与去重摘要，帮助理解历史重复、重复合并和跨来源共振。",
    "reranking_debug": "兼容旧字段。当前 topic 内顺序直接按 final_score 降序输出，不再在 merge-sources 阶段做二次重排。",
}

SCORE_ENGAGEMENT_VIRAL = 5
SCORE_ENGAGEMENT_HIGH = 3
SCORE_ENGAGEMENT_MED = 2
SCORE_ENGAGEMENT_LOW = 1
SCORE_DISCUSSION_VIRAL = 5
SCORE_DISCUSSION_HIGH = 3
SCORE_DISCUSSION_MED = 2
SCORE_DISCUSSION_LOW = 1

CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
NON_WORD_RE = re.compile(r"[^\w\s\u3400-\u4dbf\u4e00-\u9fff]+", re.UNICODE)
SPACE_RE = re.compile(r"\s+")


def resolve_article_topic(article: Dict[str, Any], default: str = "") -> str:
    topic = str(article.get("topic") or "").strip()
    return topic or default


def resolve_cluster_topic(cluster_articles: List[Dict[str, Any]], default: str = "") -> str:
    for article in cluster_articles:
        topic = resolve_article_topic(article)
        if topic:
            return topic
    return default


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


def load_source_data(file_path: Optional[Path]) -> Dict[str, Any]:
    if not file_path or not file_path.exists():
        return {"sources": [], "total_articles": 0}
    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:
        logging.warning("Failed to load %s: %s", file_path, exc)
        return {"sources": [], "total_articles": 0}


def normalize_priority(priority: Any, default: int = 3) -> int:
    if isinstance(priority, bool):
        return default
    try:
        value = int(priority)
    except (TypeError, ValueError):
        return default
    return max(1, min(10, value))


def parse_article_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=local_tzinfo())
    return to_local_datetime(parsed)


def normalize_title(title: str) -> str:
    text = unicodedata.normalize("NFKC", title or "").lower()
    text = re.sub(r"([a-z0-9])([\u3400-\u4dbf\u4e00-\u9fff])", r"\1 \2", text)
    text = re.sub(r"([\u3400-\u4dbf\u4e00-\u9fff])([a-z0-9])", r"\1 \2", text)
    text = NON_WORD_RE.sub(" ", text)
    text = SPACE_RE.sub(" ", text).strip()
    return text


def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        path = parsed.path.rstrip("/")
        return f"{domain}{path}"
    except Exception:
        return url


def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def contains_cjk(text: str) -> bool:
    return bool(CJK_RE.search(text))


def tokenize_words(text: str) -> Set[str]:
    return {token for token in normalize_title(text).split() if len(token) >= 2}


def tokenize_cjk_bigrams(text: str) -> Set[str]:
    normalized = normalize_title(text).replace(" ", "")
    chars = [ch for ch in normalized if contains_cjk(ch)]
    return {"".join(chars[i:i + 2]) for i in range(len(chars) - 1)}


def tokenize_compact_bigrams(text: str) -> Set[str]:
    compact = normalize_title(text).replace(" ", "")
    return {"".join(compact[i:i + 2]) for i in range(len(compact) - 1)}


def token_jaccard(tokens_a: Set[str], tokens_b: Set[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    union = tokens_a | tokens_b
    if not union:
        return 0.0
    return len(tokens_a & tokens_b) / len(union)


def _fallback_ratio(text_a: str, text_b: str) -> float:
    if text_a == text_b:
        return 1.0
    if not text_a or not text_b:
        return 0.0
    from difflib import SequenceMatcher
    return SequenceMatcher(None, text_a, text_b).ratio()


@lru_cache(maxsize=10000)
def rapidfuzz_ratio(kind: str, text_a: str, text_b: str) -> float:
    if not text_a or not text_b:
        return 0.0
    if fuzz is None:
        return _fallback_ratio(text_a, text_b)
    if kind == "token_set":
        return fuzz.token_set_ratio(text_a, text_b) / 100.0
    if kind == "partial":
        return fuzz.partial_ratio(text_a, text_b) / 100.0
    return fuzz.ratio(text_a, text_b) / 100.0


def build_similarity_features(article: Dict[str, Any]) -> Dict[str, Any]:
    title = article.get("title", "")
    normalized = normalize_title(title)
    compact = normalized.replace(" ", "")
    return {
        "normalized_title": normalized,
        "normalized_compact": compact,
        "word_tokens": tokenize_words(title),
        "cjk_bigrams": tokenize_cjk_bigrams(title),
        "compact_bigrams": tokenize_compact_bigrams(title),
        "normalized_url": normalize_url(article.get("link", "")),
        "domain": get_domain(article.get("link", "")),
    }


def should_compare(features_a: Dict[str, Any], features_b: Dict[str, Any]) -> bool:
    if features_a["normalized_title"] and features_a["normalized_title"] == features_b["normalized_title"]:
        return True
    if features_a["normalized_compact"] and features_a["normalized_compact"] == features_b["normalized_compact"]:
        return True
    if features_a["normalized_url"] and features_a["normalized_url"] == features_b["normalized_url"]:
        return True
    if len(features_a["word_tokens"] & features_b["word_tokens"]) >= 2:
        return True
    if len(features_a["cjk_bigrams"] & features_b["cjk_bigrams"]) >= 3:
        return True
    if len(features_a["compact_bigrams"] & features_b["compact_bigrams"]) >= 4:
        return True
    return False


def calculate_title_similarity(title1: str, title2: str) -> float:
    features_a = build_similarity_features({"title": title1, "link": ""})
    features_b = build_similarity_features({"title": title2, "link": ""})
    return calculate_similarity_from_features(features_a, features_b)


def calculate_similarity_from_features(features_a: Dict[str, Any], features_b: Dict[str, Any]) -> float:
    title_a = features_a["normalized_title"]
    title_b = features_b["normalized_title"]
    compact_a = features_a["normalized_compact"]
    compact_b = features_b["normalized_compact"]
    if compact_a and compact_a == compact_b:
        return 1.0

    token_set = max(
        rapidfuzz_ratio("token_set", title_a, title_b),
        rapidfuzz_ratio("token_set", compact_a, compact_b),
    )
    partial = max(
        rapidfuzz_ratio("partial", title_a, title_b),
        rapidfuzz_ratio("partial", compact_a, compact_b),
    )
    direct_ratio = max(
        rapidfuzz_ratio("ratio", title_a, title_b),
        rapidfuzz_ratio("ratio", compact_a, compact_b),
    )
    combined_tokens = (
        features_a["word_tokens"] | features_a["cjk_bigrams"] | features_a["compact_bigrams"],
        features_b["word_tokens"] | features_b["cjk_bigrams"] | features_b["compact_bigrams"],
    )
    jaccard = token_jaccard(*combined_tokens)

    url_hint = 0.0
    if features_a["normalized_url"] and features_a["normalized_url"] == features_b["normalized_url"]:
        url_hint = 1.0
    elif compact_a and compact_b and (
        (compact_a in compact_b and len(compact_a) / max(len(compact_b), 1) >= 0.7)
        or (compact_b in compact_a and len(compact_b) / max(len(compact_a), 1) >= 0.7)
    ):
        url_hint = 1.0
    elif features_a["domain"] and features_a["domain"] == features_b["domain"]:
        url_hint = 0.4

    score = (
        0.35 * token_set
        + 0.25 * partial
        + 0.20 * jaccard
        + 0.10 * direct_ratio
        + 0.10 * url_hint
    )
    shared_word_tokens = len(features_a["word_tokens"] & features_b["word_tokens"])
    if token_set >= 0.95 and direct_ratio >= 0.9:
        score = max(score, 0.93)
    if shared_word_tokens >= 2 and partial >= 0.85 and token_set >= 0.85:
        score = max(score, 0.8)
    return min(score, 1.0)


# ---------------------------------------------------------------------------
# Similarity, scoring, and clustering
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self, size: int):
        self.parent = list(range(size))

    def find(self, index: int) -> int:
        while self.parent[index] != index:
            self.parent[index] = self.parent[self.parent[index]]
            index = self.parent[index]
        return index

    def union(self, a: int, b: int) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a


def calculate_local_extra_score(article: Dict[str, Any], source_type: str) -> float:
    return calculate_local_extra_details(article, source_type)["score"]


def calculate_local_extra_details(article: Dict[str, Any], source_type: str) -> Dict[str, Any]:
    if source_type == "twitter":
        metrics = article.get("metrics", {})
        likes = metrics.get("like_count", 0)
        retweets = metrics.get("retweet_count", 0)
        replies = metrics.get("reply_count", 0)
        if likes >= 1000 or retweets >= 500 or replies >= 300:
            return {
                "score": float(SCORE_ENGAGEMENT_VIRAL),
                "rule": "twitter_engagement",
                "tier": "viral",
                "matched_on": {"likes": likes, "retweets": retweets, "replies": replies},
            }
        if likes >= 500 or retweets >= 200 or replies >= 150:
            return {
                "score": float(SCORE_ENGAGEMENT_HIGH),
                "rule": "twitter_engagement",
                "tier": "high",
                "matched_on": {"likes": likes, "retweets": retweets, "replies": replies},
            }
        if likes >= 100 or retweets >= 50 or replies >= 60:
            return {
                "score": float(SCORE_ENGAGEMENT_MED),
                "rule": "twitter_engagement",
                "tier": "medium",
                "matched_on": {"likes": likes, "retweets": retweets, "replies": replies},
            }
        if likes >= 50 or retweets >= 20 or replies >= 20:
            return {
                "score": float(SCORE_ENGAGEMENT_LOW),
                "rule": "twitter_engagement",
                "tier": "low",
                "matched_on": {"likes": likes, "retweets": retweets, "replies": replies},
            }
        return {
            "score": 0.0,
            "rule": "twitter_engagement",
            "tier": "none",
            "matched_on": {"likes": likes, "retweets": retweets, "replies": replies},
        }

    if source_type == "reddit":
        score = int(article.get("score", 0) or 0)
        comments = int(article.get("num_comments", 0) or 0)
        if score >= 1000 or comments >= 300:
            return {
                "score": float(SCORE_DISCUSSION_VIRAL),
                "rule": "reddit_discussion",
                "tier": "viral",
                "matched_on": {"score": score, "comments": comments},
            }
        if score >= 500 or comments >= 150:
            return {
                "score": float(SCORE_DISCUSSION_HIGH),
                "rule": "reddit_discussion",
                "tier": "high",
                "matched_on": {"score": score, "comments": comments},
            }
        if score >= 200 or comments >= 80:
            return {
                "score": float(SCORE_DISCUSSION_MED),
                "rule": "reddit_discussion",
                "tier": "medium",
                "matched_on": {"score": score, "comments": comments},
            }
        if score >= 100 or comments >= 30:
            return {
                "score": float(SCORE_DISCUSSION_LOW),
                "rule": "reddit_discussion",
                "tier": "low",
                "matched_on": {"score": score, "comments": comments},
            }
        return {
            "score": 0.0,
            "rule": "reddit_discussion",
            "tier": "none",
            "matched_on": {"score": score, "comments": comments},
        }

    if source_type == "v2ex":
        replies = article.get("replies", 0)
        score = float(calculate_v2ex_replies_score(replies))
        return {
            "score": score,
            "rule": "v2ex_replies",
            "tier": "none" if score == 0 else "matched",
            "matched_on": {"replies": replies},
        }

    return {"score": 0.0, "rule": "unsupported", "tier": "none", "matched_on": {}}


def calculate_v2ex_replies_score(replies: Any) -> int:
    try:
        replies_int = int(replies)
    except (TypeError, ValueError):
        return 0
    if replies_int >= 200:
        return SCORE_DISCUSSION_VIRAL
    if replies_int >= 100:
        return SCORE_DISCUSSION_HIGH
    if replies_int >= 50:
        return SCORE_DISCUSSION_MED
    if replies_int >= 20:
        return SCORE_DISCUSSION_LOW
    return 0


def calculate_recency_score(article: Dict[str, Any]) -> float:
    return calculate_recency_score_details(article)["score"]


def calculate_recency_score_details(article: Dict[str, Any]) -> Dict[str, Any]:
    article_date = parse_article_datetime(article.get("date"))
    if article_date is None:
        return {"score": 0.0, "hours_old": None, "bucket": "missing_date"}

    hours_old = (local_now() - article_date).total_seconds() / 3600
    if hours_old < 6:
        return {
            "score": SCORING_CONFIG["recency_24h_score"] + SCORING_CONFIG["recency_6h_score"],
            "hours_old": round(hours_old, 3),
            "bucket": "under_6h",
        }
    if hours_old < 24:
        return {
            "score": SCORING_CONFIG["recency_24h_score"],
            "hours_old": round(hours_old, 3),
            "bucket": "under_24h",
        }
    return {"score": 0.0, "hours_old": round(hours_old, 3), "bucket": "older"}


def initialize_article_scores(articles: List[Dict[str, Any]]) -> None:
    for article in articles:
        source_type = article.get("source_type", "")
        base_priority_score = float(normalize_priority(article.get("source_priority", 3)))
        local_extra_details = calculate_local_extra_details(article, source_type)
        local_extra_score = local_extra_details["score"]
        article["_score_components"] = {
            "base_priority_score": base_priority_score,
            "local_extra_score": local_extra_score,
            "fetch_local_rank_score": 0.0,
            "history_score": 0.0,
            "cross_source_hot_score": 0.0,
            "recency_score": 0.0,
        }
        article["similarity_debug"] = {
            "_comment": SCORE_DEBUG_COMMENTS["similarity_debug"],
            "history_similarity": 0.0,
            "history_duplicate": False,
            "duplicate_group": {
                "merged": False,
                "cluster_size": 1,
            },
            "cross_source_hot": {
                "matched_source_type_count": 0,
                "score": 0.0,
            },
            "fields_comment_zh": {
                "history_similarity": "与历史热点标题的最高相似度，范围通常为 0 到 1。",
                "history_duplicate": "是否因为命中历史重复分值规则而被判定为历史重复内容。",
                "duplicate_group": "当前文章是否与其他文章合并，以及合并后的重复簇大小。",
                "cross_source_hot": "当前文章被多个不同 source_type 同时命中时，对应的命中数和分值影响。",
            },
        }
        article["scoring_debug"] = {
            "_comment": SCORE_DEBUG_COMMENTS["scoring_debug"],
            "final_score_formula": "base_priority_score + fetch_local_rank_score + history_score + cross_source_hot_score + recency_score",
        }
        article["final_score"] = base_priority_score


def assign_fetch_rank_scores(articles: List[Dict[str, Any]]) -> None:
    fetch_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for article in articles:
        fetch_groups[article.get("source_type", "unknown")].append(article)

    for fetch_type, group in fetch_groups.items():
        ordered = sorted(
            group,
            key=lambda item: (
                -(item["_score_components"]["base_priority_score"] + item["_score_components"]["local_extra_score"]),
                item.get("title", ""),
            ),
        )
        total = len(ordered)
        for rank, article in enumerate(ordered, start=1):
            if total <= 1:
                rank_pct = 1.0
            else:
                rank_pct = 1.0 - ((rank - 1) / (total - 1))
            rank_score = round(SCORING_CONFIG["fetch_rank_max"] * rank_pct, 3)
            article["_score_components"]["fetch_local_rank_score"] = rank_score
        logging.debug("Assigned fetch-local rank scores for %s (%d items)", fetch_type, total)


def build_previous_title_features(previous_titles: Iterable[str]) -> List[Dict[str, Any]]:
    return [
        {"raw": title, **build_similarity_features({"title": title, "link": ""})}
        for title in previous_titles
    ]


def best_history_similarity(article: Dict[str, Any], previous_title_features: List[Dict[str, Any]]) -> float:
    if not previous_title_features:
        return 0.0
    article_features = article["_similarity_features"]
    best = 0.0
    for previous in previous_title_features:
        best = max(best, calculate_similarity_from_features(article_features, previous))
    return best


def history_score_for_similarity(value: float) -> float:
    for threshold, score in SCORING_CONFIG["history_scores"]:
        if value >= threshold:
            return score
    return 0.0


def apply_history_scores(articles: List[Dict[str, Any]], previous_titles: Iterable[str]) -> None:
    previous_features = build_previous_title_features(previous_titles)
    for article in articles:
        similarity = best_history_similarity(article, previous_features)
        score = history_score_for_similarity(similarity)
        article["similarity_debug"]["history_similarity"] = round(similarity, 4)
        article["_score_components"]["history_score"] = score
        if score < 0:
            article["similarity_debug"]["history_duplicate"] = True


def build_candidate_pairs(articles: List[Dict[str, Any]]) -> Iterable[Tuple[int, int]]:
    word_buckets: Dict[str, List[int]] = defaultdict(list)
    cjk_buckets: Dict[str, List[int]] = defaultdict(list)
    url_buckets: Dict[str, List[int]] = defaultdict(list)
    title_buckets: Dict[str, List[int]] = defaultdict(list)
    compact_buckets: Dict[str, List[int]] = defaultdict(list)

    for idx, article in enumerate(articles):
        features = article["_similarity_features"]
        for token in features["word_tokens"]:
            word_buckets[token].append(idx)
        for token in features["cjk_bigrams"]:
            cjk_buckets[token].append(idx)
        if features["normalized_url"]:
            url_buckets[features["normalized_url"]].append(idx)
        if features["normalized_title"]:
            title_buckets[features["normalized_title"]].append(idx)
        if features["normalized_compact"]:
            compact_buckets[features["normalized_compact"]].append(idx)

    pair_counts: Dict[Tuple[int, int], int] = defaultdict(int)
    for bucket in word_buckets.values():
        if len(bucket) < 2:
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                pair = (bucket[i], bucket[j])
                pair_counts[pair] += 1

    cjk_counts: Dict[Tuple[int, int], int] = defaultdict(int)
    for bucket in cjk_buckets.values():
        if len(bucket) < 2:
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                pair = (bucket[i], bucket[j])
                cjk_counts[pair] += 1

    yielded: Set[Tuple[int, int]] = set()
    for pair, count in pair_counts.items():
        if count >= 2:
            yielded.add(pair)
            yield pair
    for pair, count in cjk_counts.items():
        if count >= 3 and pair not in yielded:
            yielded.add(pair)
            yield pair
    for bucket in url_buckets.values():
        if len(bucket) < 2:
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                pair = (bucket[i], bucket[j])
                if pair not in yielded:
                    yielded.add(pair)
                    yield pair
    for bucket in title_buckets.values():
        if len(bucket) < 2:
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                pair = (bucket[i], bucket[j])
                if pair not in yielded:
                    yielded.add(pair)
                    yield pair
    for bucket in compact_buckets.values():
        if len(bucket) < 2:
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                pair = (bucket[i], bucket[j])
                if pair not in yielded:
                    yielded.add(pair)
                    yield pair


def apply_cross_source_hot_scores(articles: List[Dict[str, Any]], pair_similarities: Dict[Tuple[int, int], float]) -> None:
    hot_union = UnionFind(len(articles))
    for (i, j), similarity in pair_similarities.items():
        if similarity < SCORING_CONFIG["cross_source_hot_threshold"]:
            continue
        if articles[i].get("source_type") == articles[j].get("source_type"):
            continue
        hot_union.union(i, j)

    hot_groups: Dict[int, List[int]] = defaultdict(list)
    for idx in range(len(articles)):
        hot_groups[hot_union.find(idx)].append(idx)

    for indices in hot_groups.values():
        source_types = {articles[idx].get("source_type", "") for idx in indices if articles[idx].get("source_type")}
        extra_types = max(0, len(source_types) - 1)
        score = min(
            SCORING_CONFIG["cross_source_hot_score_cap"],
            SCORING_CONFIG["cross_source_hot_score_per_extra_type"] * extra_types,
        )
        for idx in indices:
            articles[idx]["_score_components"]["cross_source_hot_score"] = score
            articles[idx]["similarity_debug"]["cross_source_hot"]["matched_source_type_count"] = extra_types


def recalculate_final_scores(articles: List[Dict[str, Any]]) -> None:
    for article in articles:
        score_components = article["_score_components"]
        recency_details = calculate_recency_score_details(article)
        score_components["recency_score"] = recency_details["score"]
        final_score = (
            score_components["base_priority_score"]
            + score_components["fetch_local_rank_score"]
            + score_components["history_score"]
            + score_components["cross_source_hot_score"]
            + score_components["recency_score"]
        )
        article["final_score"] = round(final_score, 3)
        article["scoring_debug"]["final_score"] = {
            "_comment": SCORE_DEBUG_COMMENTS["final_score"],
            "value": article["final_score"],
            "components": {
                "base_priority_score": score_components["base_priority_score"],
                "local_extra_score": score_components["local_extra_score"],
                "fetch_local_rank_score": score_components["fetch_local_rank_score"],
                "history_score": score_components["history_score"],
                "cross_source_hot_score": score_components["cross_source_hot_score"],
                "recency_score": score_components["recency_score"],
            },
            "components_comment_zh": {
                "base_priority_score": "基础分，来自 source_priority。",
                "local_extra_score": "源内热度参考分，用于解释该条内容的站内热度信号。",
                "fetch_local_rank_score": "同 source_type 内按排序位置得到的分值影响。",
                "history_score": "与历史热点相似时带来的分值影响，重复时通常为负分。",
                "cross_source_hot_score": "被多个不同 source_type 命中时的分值影响。",
                "recency_score": "时效性带来的分值影响。",
            },
        }
        article["similarity_debug"]["cross_source_hot"]["score"] = score_components["cross_source_hot_score"]


def build_output_scoring_config() -> Dict[str, Any]:
    return {
        "fetch_rank_max_score": SCORING_CONFIG["fetch_rank_max"],
        "history_threshold": SCORING_CONFIG["history_threshold"],
        "history_score_rules": [
            {"threshold": threshold, "score": score}
            for threshold, score in SCORING_CONFIG["history_scores"]
        ],
        "cross_source_hot_threshold": SCORING_CONFIG["cross_source_hot_threshold"],
        "duplicate_threshold": SCORING_CONFIG["duplicate_threshold"],
        "cross_source_hot_score_per_extra_type": SCORING_CONFIG["cross_source_hot_score_per_extra_type"],
        "cross_source_hot_score_cap": SCORING_CONFIG["cross_source_hot_score_cap"],
        "recency_24h_score": SCORING_CONFIG["recency_24h_score"],
        "recency_6h_score": SCORING_CONFIG["recency_6h_score"],
    }


def _compute_pair_similarity(args: Tuple[int, int, Dict[str, Any], Dict[str, Any]]) -> Tuple[Tuple[int, int], float]:
    """Worker function for parallel similarity computation."""
    (i, j, features_i, features_j) = args
    similarity = calculate_similarity_from_features(features_i, features_j)
    return ((i, j), similarity)


def apply_similarity_scoring(articles: List[Dict[str, Any]], previous_titles: Iterable[str]) -> Dict[Tuple[int, int], float]:
    for article in articles:
        article["_similarity_features"] = build_similarity_features(article)

    assign_fetch_rank_scores(articles)
    apply_history_scores(articles, previous_titles)

    # 收集所有候选对
    candidate_pairs = []
    for i, j in build_candidate_pairs(articles):
        features_i = articles[i]["_similarity_features"]
        features_j = articles[j]["_similarity_features"]
        if should_compare(features_i, features_j):
            candidate_pairs.append((i, j, features_i, features_j))

    # 并行计算相似度
    pair_similarities: Dict[Tuple[int, int], float] = {}
    max_workers = min(4, (os.cpu_count() or 2))
    
    if len(candidate_pairs) > 100:
        # 大数据量用并行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_compute_pair_similarity, pair) for pair in candidate_pairs]
            for future in as_completed(futures):
                (i, j), sim = future.result()
                pair_similarities[(i, j)] = sim
    else:
        # 小数据量直接串行（避免线程开销）
        for pair in candidate_pairs:
            (i, j), sim = _compute_pair_similarity(pair)
            pair_similarities[(i, j)] = sim

    apply_cross_source_hot_scores(articles, pair_similarities)
    recalculate_final_scores(articles)
    return pair_similarities


def merge_cluster_metadata(canonical: Dict[str, Any], cluster_articles: List[Dict[str, Any]], cluster_id: int) -> Dict[str, Any]:
    canonical["multi_source"] = len({a.get("source_type") for a in cluster_articles}) > 1
    canonical["similarity_debug"]["duplicate_group"] = {
        "merged": len(cluster_articles) > 1,
        "cluster_size": len(cluster_articles),
    }

    canonical_topic = resolve_article_topic(canonical)
    if canonical_topic:
        pass
    else:
        merged_topic = resolve_cluster_topic(cluster_articles, default="")
        if merged_topic:
            canonical["topic"] = merged_topic
    
    return canonical


def deduplicate_articles(articles: List[Dict[str, Any]], previous_titles: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    if not articles:
        return articles

    for article in articles:
        article.setdefault("source_priority", normalize_priority(article.get("source_priority", article.get("priority", 3))))
    initialize_article_scores(articles)
    pair_similarities = apply_similarity_scoring(articles, previous_titles or [])

    duplicate_union = UnionFind(len(articles))

    for (i, j), similarity in pair_similarities.items():
        if similarity >= SCORING_CONFIG["duplicate_threshold"]:
            duplicate_union.union(i, j)
        elif (
            articles[i]["_similarity_features"]["normalized_url"]
            and articles[i]["_similarity_features"]["normalized_url"] == articles[j]["_similarity_features"]["normalized_url"]
        ):
            duplicate_union.union(i, j)

    clusters: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for idx, article in enumerate(articles):
        clusters[duplicate_union.find(idx)].append(article)

    deduplicated = []
    for cluster_id, cluster_articles in clusters.items():
        canonical = max(
            cluster_articles,
            key=lambda item: (
                item.get("final_score", 0),
                item["_score_components"]["local_extra_score"],
                item.get("title", ""),
            ),
        )
        canonical = merge_cluster_metadata(canonical, cluster_articles, cluster_id)
        deduplicated.append(canonical)

    for article in deduplicated:
        article.pop("_similarity_features", None)

    deduplicated.sort(key=lambda item: item.get("final_score", 0), reverse=True)
    logging.info("Deduplication: %d → %d articles", len(articles), len(deduplicated))
    return deduplicated


def load_previous_hotspots(archive_dir: Path, days: int = 14) -> List[str]:
    if not archive_dir.exists():
        return []

    seen_titles: List[str] = []
    cutoff_date = (local_now() - timedelta(days=days)).date()
    try:
        for file_path in sorted(archive_dir.rglob("daily*.json")):
            if file_path.parent.name != "json":
                continue
            match = re.search(r"(\d{4}-\d{2}-\d{2})", str(file_path))
            if match:
                try:
                    file_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                    if file_date < cutoff_date:
                        continue
                except ValueError:
                    continue
            with open(file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            for topic in data.get("topics", []):
                for item in topic.get("items", []):
                    title = str(item.get("title", "")).strip()
                    if title:
                        seen_titles.append(title)
    except Exception as exc:
        logging.debug("Failed to load previous hotspots: %s", exc)

    logging.info("Loaded %d titles from previous %d days under %s", len(seen_titles), days, archive_dir)
    return seen_titles


def group_by_source_types(articles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    source_groups: Dict[str, List[Dict[str, Any]]] = {}
    for article in articles:
        source_type = str(article.get("source_type", "") or "unknown")
        source_groups.setdefault(source_type, []).append(article.copy())

    for source_type, source_articles in source_groups.items():
        source_groups[source_type] = sorted(
            source_articles,
            key=lambda item: item.get("final_score", 0),
            reverse=True,
        )
    return source_groups


def load_articles_payload(file_path: Optional[Path], source_type: str) -> Dict[str, Any]:
    payload = load_source_data(file_path)
    articles = payload.get("articles", [])
    if not isinstance(articles, list):
        articles = []

    normalized_articles: List[Dict[str, Any]] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        normalized = article.copy()
        normalized["source_type"] = str(normalized.get("source_type") or source_type)
        normalized["source_priority"] = normalize_priority(normalized.get("source_priority", normalized.get("priority", 3)))
        normalized["topic"] = resolve_article_topic(normalized, default="uncategorized") or "uncategorized"
        normalized_articles.append(normalized)

    return {"generated": payload.get("generated"), "articles": normalized_articles}


def load_input_payloads(args: argparse.Namespace) -> Dict[str, Dict[str, Any]]:
    return {
        step.step_key: load_articles_payload(getattr(args, step.step_key), step.source_type)
        for step in ALL_SOURCE_STEPS
    }


def collect_articles(payloads: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_articles: List[Dict[str, Any]] = []
    for step_key in STEP_KEYS:
        all_articles.extend(
            article.copy()
            for article in payloads.get(step_key, {}).get("articles", [])
            if isinstance(article, dict)
        )
    return all_articles


def build_input_stats(payloads: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    distribution = {
        step_key: len(payloads.get(step_key, {}).get("articles", []))
        for step_key in STEP_KEYS
    }
    return {
        "total_articles": sum(distribution.values()),
        "source_type_distribution": distribution,
    }


def serialize_article_for_output(article: Dict[str, Any]) -> Dict[str, Any]:
    score_components = article.get("_score_components", {}) if isinstance(article.get("_score_components"), dict) else {}
    output = {
        "title": article.get("title"),
        "link": article.get("link"),
        "date": article.get("date"),
        "topic": article.get("topic"),
        "source_type": article.get("source_type"),
        "source_id": article.get("source_id"),
        "source_name": article.get("source_name"),
        "source_priority": article.get("source_priority"),
        "final_score": article.get("final_score"),
        "score_components": {
            "base_priority_score": score_components.get("base_priority_score", 0.0),
            "fetch_local_rank_score": score_components.get("fetch_local_rank_score", 0.0),
            "history_score": score_components.get("history_score", 0.0),
            "cross_source_hot_score": score_components.get("cross_source_hot_score", 0.0),
            "recency_score": score_components.get("recency_score", 0.0),
            "local_extra_score": score_components.get("local_extra_score", 0.0),
        },
    }

    optional_fields = (
        "summary",
        "snippet",
        "metrics",
        "replies",
        "num_comments",
        "score",
        "reddit_url",
        "external_url",
        "name",
        "repo",
        "published_at",
    )
    for field in optional_fields:
        if field in article:
            output[field] = article.get(field)
    return output


def build_merged_output(
    payloads: Dict[str, Dict[str, Any]],
    source_groups: Dict[str, List[Dict[str, Any]]],
    previous_titles: List[str],
    total_collected: int,
) -> Dict[str, Any]:
    distribution = {source_type: len(items) for source_type, items in source_groups.items()}
    return {
        "generated": now_iso(),
        "input_stats": build_input_stats(payloads),
        "output_stats": {
            "total_articles": sum(distribution.values()),
            "source_types_count": len(source_groups),
            "source_type_distribution": distribution,
        },
        "processing": {
            "deduplication_applied": True,
            "previous_hotspots_scoring_applied": len(previous_titles) > 0,
            "scoring_applied": True,
            "scoring_version": "2.0",
            "score_formula": "base_priority_score + fetch_local_rank_score + history_score + cross_source_hot_score + recency_score",
            "scoring_config": build_output_scoring_config(),
            "input_total_articles": total_collected,
        },
        "source_types": {
            source_type: {
                "count": len(items),
                "articles": [serialize_article_for_output(article) for article in items],
            }
            for source_type, items in source_groups.items()
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge standardized fetch outputs with scoring and deduplication.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    for step in ALL_SOURCE_STEPS:
        parser.add_argument(step.merge_arg, dest=step.step_key, type=Path, help=f"{step.display_name} fetch results JSON file")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path (default: auto-generated temp file)")
    parser.add_argument("--archive", dest="archive_dir", type=Path, help="Archive directory for previous hotspots scoring history")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-merged-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        payloads = load_input_payloads(args)
        total_collected = sum(len(payload.get("articles", [])) for payload in payloads.values())
        logger.info("Loaded %d standardized articles", total_collected)
        previous_titles: List[str] = load_previous_hotspots(args.archive_dir) if args.archive_dir else []
        deduplicated_articles = deduplicate_articles(collect_articles(payloads), previous_titles)
        source_groups = group_by_source_types(deduplicated_articles)
        output = build_merged_output(payloads, source_groups, previous_titles, total_collected)

        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(output, handle, ensure_ascii=False, indent=2)

        logger.info("✅ Merged and scored articles:")
        logger.info("   Input: %d articles", total_collected)
        logger.info("   Output: %d articles across %d source types", output["output_stats"]["total_articles"], len(source_groups))
        logger.info("   File: %s", args.output)
        return 0
    except Exception as exc:
        logger.error("💥 Merge failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
