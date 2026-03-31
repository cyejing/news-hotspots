#!/usr/bin/env python3
"""
Merge data from enabled fetch steps with layered scoring and deduplication.

Reads output from fetch-rss.py, fetch-twitter.py, fetch-google.py,
fetch-github.py, fetch-github-trending.py, fetch-api.py, fetch-reddit.py,
fetch-v2ex.py, fetch-zhihu.py, fetch-weibo.py, fetch-toutiao.py, and any other compatible JSON inputs that are provided.
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

try:
    from topic_utils import resolve_primary_topic
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from topic_utils import resolve_primary_topic

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover - defensive fallback
    fuzz = None


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
    "topic_same_source_score": -1.5,
    "topic_same_domain_score": -0.75,
    "topic_first3_source_score": -3.0,
    "topic_first3_domain_score": -1.5,
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

DOMAIN_LIMIT_EXEMPT = {"x.com", "twitter.com", "github.com", "reddit.com"}
CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
NON_WORD_RE = re.compile(r"[^\w\s\u3400-\u4dbf\u4e00-\u9fff]+", re.UNICODE)
SPACE_RE = re.compile(r"\s+")


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
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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

    hours_old = (datetime.now(timezone.utc) - article_date).total_seconds() / 3600
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
        "topic_same_source_score": SCORING_CONFIG["topic_same_source_score"],
        "topic_same_domain_score": SCORING_CONFIG["topic_same_domain_score"],
        "topic_first3_source_score": SCORING_CONFIG["topic_first3_source_score"],
        "topic_first3_domain_score": SCORING_CONFIG["topic_first3_domain_score"],
    }


def apply_similarity_scoring(articles: List[Dict[str, Any]], previous_titles: Iterable[str]) -> Dict[Tuple[int, int], float]:
    for article in articles:
        article["_similarity_features"] = build_similarity_features(article)

    assign_fetch_rank_scores(articles)
    apply_history_scores(articles, previous_titles)

    pair_similarities: Dict[Tuple[int, int], float] = {}
    for i, j in build_candidate_pairs(articles):
        features_i = articles[i]["_similarity_features"]
        features_j = articles[j]["_similarity_features"]
        if not should_compare(features_i, features_j):
            continue
        pair_similarities[(i, j)] = calculate_similarity_from_features(features_i, features_j)

    apply_cross_source_hot_scores(articles, pair_similarities)
    recalculate_final_scores(articles)
    return pair_similarities


def merge_cluster_metadata(canonical: Dict[str, Any], cluster_articles: List[Dict[str, Any]], cluster_id: int) -> Dict[str, Any]:
    unique_sources = []
    seen = set()
    for article in cluster_articles:
        source_name = article.get("source_name") or article.get("source_id") or article.get("source_type")
        if source_name and source_name not in seen:
            seen.add(source_name)
            unique_sources.append(source_name)

    canonical["multi_source"] = len({a.get("source_type") for a in cluster_articles}) > 1
    canonical["source_name_count"] = len(unique_sources)
    canonical["source_names"] = unique_sources[:5]
    canonical["similarity_debug"]["duplicate_group"] = {
        "merged": len(cluster_articles) > 1,
        "cluster_size": len(cluster_articles),
    }

    merged_topic = resolve_primary_topic(cluster_articles, default=canonical.get("topic", ""))
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


def apply_domain_limits(articles: List[Dict[str, Any]], max_per_domain: int = 3) -> List[Dict[str, Any]]:
    domain_counts: Dict[str, int] = {}
    result = []
    for article in articles:
        domain = get_domain(article.get("link", ""))
        if domain and domain not in DOMAIN_LIMIT_EXEMPT:
            count = domain_counts.get(domain, 0)
            if count >= max_per_domain:
                logging.debug("Domain limit (%d): skipping %s article", max_per_domain, domain)
                continue
            domain_counts[domain] = count + 1
        result.append(article)
    return result


# ---------------------------------------------------------------------------
# History loading and topic grouping
# ---------------------------------------------------------------------------

def load_previous_hotspots(archive_dir: Path, days: int = 14) -> List[str]:
    if not archive_dir.exists():
        return []

    seen_titles: List[str] = []
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).date()
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
            if not file_path.name.startswith("daily"):
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


def group_by_topics(articles: List[Dict[str, Any]], dedup_across_topics: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    topic_groups: Dict[str, List[Dict[str, Any]]] = {}
    seen_article_ids: Set[str] = set()

    for article in articles:
        primary_topic = resolve_primary_topic(article, default="uncategorized") or "uncategorized"
        article_id = normalize_title(article.get("title", ""))

        if dedup_across_topics and article_id in seen_article_ids:
            continue
        seen_article_ids.add(article_id)

        topic_groups.setdefault(primary_topic, [])
        article_copy = article.copy()
        article_copy["topic"] = primary_topic
        topic_groups[primary_topic].append(article_copy)

    for topic, topic_articles in topic_groups.items():
        topic_groups[topic] = sorted(
            topic_articles,
            key=lambda item: item.get("final_score", 0),
            reverse=True,
        )

    return topic_groups


def group_by_source_types(articles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    source_groups: Dict[str, List[Dict[str, Any]]] = {}

    for article in articles:
        source_type = str(article.get("source_type", "") or "unknown")
        source_groups.setdefault(source_type, [])
        source_groups[source_type].append(article.copy())

    for source_type, source_articles in source_groups.items():
        source_groups[source_type] = sorted(
            source_articles,
            key=lambda item: item.get("final_score", 0),
            reverse=True,
        )

    return source_groups


# ---------------------------------------------------------------------------
# Input normalization
# ---------------------------------------------------------------------------

def build_article(title: str, link: str, date: str, source_type: str, source_name: str, source_id: str, source_priority: int, topic: str, **extra: Any) -> Dict[str, Any]:
    article = {
        "title": title,
        "link": link,
        "date": date,
        "source_type": source_type,
        "source_name": source_name,
        "source_id": source_id,
        "source_priority": source_priority,
        "topic": topic,
    }
    article.update(extra)
    return article


def collect_articles(
    rss_data: Dict[str, Any],
    twitter_data: Dict[str, Any],
    google_data: Dict[str, Any],
    github_data: Dict[str, Any],
    trending_data: Dict[str, Any],
    reddit_data: Dict[str, Any],
    api_data: Dict[str, Any],
    v2ex_data: Dict[str, Any],
    zhihu_data: Dict[str, Any],
    weibo_data: Dict[str, Any],
    toutiao_data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    all_articles: List[Dict[str, Any]] = []

    for source in rss_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "rss",
                "source_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for source in twitter_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "twitter",
                "source_name": f"@{source.get('handle', '')}",
                "display_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for topic_result in twitter_data.get("topics", []):
        for article in topic_result.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "twitter",
                "source_name": "Twitter Search",
                "source_id": f"twitter-{topic_result.get('topic_id', '')}",
                "source_priority": 3,
            })
            all_articles.append(enriched)

    for topic_result in google_data.get("topics", []):
        for article in topic_result.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "google",
                "source_name": "Google News",
                "source_id": f"google-{topic_result.get('topic_id', '')}",
                "source_priority": 3,
            })
            all_articles.append(enriched)

    for source in github_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "github",
                "source_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    reddit_source_records = reddit_data.get("sources", [])
    if not isinstance(reddit_source_records, list) or not reddit_source_records:
        reddit_source_records = reddit_data.get("subreddits", [])

    for source in reddit_source_records:
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "reddit",
                "source_name": f"r/{source.get('subreddit', '')}",
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for topic_result in reddit_data.get("topics", []):
        for article in topic_result.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "reddit",
                "source_name": "Reddit Search",
                "source_id": f"reddit-{topic_result.get('topic_id', '')}",
                "source_priority": 3,
            })
            all_articles.append(enriched)

    for source in api_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "api",
                "source_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for source in v2ex_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "v2ex",
                "source_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for source in zhihu_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "zhihu",
                "source_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for source in weibo_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "weibo",
                "source_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for source in toutiao_data.get("sources", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "toutiao",
                "source_name": source.get("name", ""),
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
            })
            all_articles.append(enriched)

    for repo in trending_data.get("repos", []):
        all_articles.append(
            build_article(
                title=f"{repo['repo']}: {repo['description']}" if repo.get("description") else repo["repo"],
                link=repo.get("url", f"https://github.com/{repo['repo']}"),
                date=repo.get("pushed_at", ""),
                source_type="github_trending",
                source_name="GitHub Trending",
                source_id=f"trending-{repo.get('repo', '')}",
                source_priority=4,
                topic=resolve_primary_topic(repo, default="github"),
                snippet=repo.get("description", ""),
                stars=repo.get("stars", 0),
                daily_stars_est=repo.get("daily_stars_est", 0),
                forks=repo.get("forks", 0),
                language=repo.get("language", ""),
            )
        )

    return all_articles


def load_input_payloads(args: argparse.Namespace) -> Dict[str, Dict[str, Any]]:
    return {
        "rss": load_source_data(args.rss),
        "twitter": load_source_data(args.twitter),
        "google": load_source_data(args.google),
        "github": load_source_data(args.github),
        "trending": load_source_data(args.trending),
        "reddit": load_source_data(args.reddit),
        "api": load_source_data(args.api),
        "v2ex": load_source_data(args.v2ex),
        "zhihu": load_source_data(args.zhihu),
        "weibo": load_source_data(args.weibo),
        "toutiao": load_source_data(args.toutiao),
    }


def log_input_summary(payloads: Dict[str, Dict[str, Any]]) -> None:
    logging.info(
        "Loaded sources - RSS: %s, Twitter: %s, Google: %s, GitHub: %s + %s trending, Reddit: %s, API: %s, V2EX: %s, Zhihu: %s, Weibo: %s, Toutiao: %s",
        payloads["rss"].get("total_articles", 0),
        payloads["twitter"].get("total_articles", 0),
        payloads["google"].get("total_articles", 0),
        payloads["github"].get("total_articles", 0),
        payloads["trending"].get("total", 0),
        payloads["reddit"].get("total_posts", 0),
        payloads["api"].get("total_articles", 0),
        payloads["v2ex"].get("total_articles", 0),
        payloads["zhihu"].get("total_articles", 0),
        payloads["weibo"].get("total_articles", 0),
        payloads["toutiao"].get("total_articles", 0),
    )


def process_articles(
    payloads: Dict[str, Dict[str, Any]],
    archive_dir: Optional[Path],
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str], int]:
    all_articles = collect_articles(
        payloads["rss"],
        payloads["twitter"],
        payloads["google"],
        payloads["github"],
        payloads["trending"],
        payloads["reddit"],
        payloads["api"],
        payloads["v2ex"],
        payloads["zhihu"],
        payloads["weibo"],
        payloads["toutiao"],
    )
    total_collected = len(all_articles)
    logging.info("Total articles collected: %d", total_collected)

    previous_titles: List[str] = load_previous_hotspots(archive_dir) if archive_dir else []
    deduplicated_articles = deduplicate_articles(all_articles, previous_titles)
    topic_groups = group_by_topics(deduplicated_articles, dedup_across_topics=True)

    for topic, topic_articles in list(topic_groups.items()):
        before = len(topic_articles)
        topic_groups[topic] = apply_domain_limits(topic_articles)
        after = len(topic_groups[topic])
        if before != after:
            logging.info("Domain limits (%s): %d → %d", topic, before, after)

    return topic_groups, previous_titles, total_collected


# ---------------------------------------------------------------------------
# Output assembly
# ---------------------------------------------------------------------------

def build_merged_output(
    payloads: Dict[str, Dict[str, Any]],
    source_groups: Dict[str, List[Dict[str, Any]]],
    previous_titles: List[str],
    total_collected: int,
) -> Dict[str, Any]:
    total_after_domain_limits = sum(len(items) for items in source_groups.values())
    source_type_counts = {source_type: len(items) for source_type, items in source_groups.items()}
    serialized_source_types = {
        source_type: {
            "count": len(items),
            "articles": [serialize_article_for_output(article) for article in items],
        }
        for source_type, items in source_groups.items()
    }
    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "input_sources": {
            "rss_articles": payloads["rss"].get("total_articles", 0),
            "twitter_articles": payloads["twitter"].get("total_articles", 0),
            "google_articles": payloads["google"].get("total_articles", 0),
            "github_articles": payloads["github"].get("total_articles", 0),
            "github_trending": payloads["trending"].get("total", 0),
            "reddit_posts": payloads["reddit"].get("total_posts", 0),
            "api_articles": payloads["api"].get("total_articles", 0),
            "v2ex_articles": payloads["v2ex"].get("total_articles", 0),
            "zhihu_articles": payloads["zhihu"].get("total_articles", 0),
            "weibo_articles": payloads["weibo"].get("total_articles", 0),
            "toutiao_articles": payloads["toutiao"].get("total_articles", 0),
            "total_input": total_collected,
        },
        "processing": {
            "deduplication_applied": True,
            "multi_source_merging": True,
            "previous_hotspots_scoring_applied": len(previous_titles) > 0,
            "scoring_applied": True,
            "scoring_version": "2.0",
            "scoring_comment_zh": "merge-sources 会先算每条内容的合并分，并按 source_type 分组后在组内按 final_score 降序输出。",
            "score_formula": {
                "merge_score": "base_priority_score + fetch_local_rank_score + history_score + cross_source_hot_score + recency_score",
                "source_type_grouping": "group_by_source_type_then_sort_by_final_score_desc",
            },
            "scoring_config": build_output_scoring_config(),
        },
        "output_stats": {
            "total_articles": total_after_domain_limits,
            "source_types_count": len(source_groups),
            "source_type_distribution": source_type_counts,
        },
        "source_types": {source_type: payload for source_type, payload in serialized_source_types.items()},
    }


def serialize_article_for_output(article: Dict[str, Any]) -> Dict[str, Any]:
    output = dict(article)
    output.pop("_score_components", None)
    output.pop("quality_score", None)
    output.pop("cluster_size", None)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge articles from all sources with quality scoring and deduplication.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--rss", type=Path, help="RSS fetch results JSON file")
    parser.add_argument("--twitter", type=Path, help="Twitter fetch results JSON file")
    parser.add_argument("--google", type=Path, help="Google News results JSON file")
    parser.add_argument("--web", dest="google", type=Path, help="Legacy alias for Google News results JSON file")
    parser.add_argument("--github", type=Path, help="GitHub releases results JSON file")
    parser.add_argument("--trending", type=Path, help="GitHub trending repos JSON file")
    parser.add_argument("--reddit", type=Path, help="Reddit posts results JSON file")
    parser.add_argument("--api", type=Path, help="API sources results JSON file")
    parser.add_argument("--v2ex", type=Path, help="V2EX hot topics results JSON file")
    parser.add_argument("--zhihu", type=Path, help="Zhihu hot topics results JSON file")
    parser.add_argument("--weibo", type=Path, help="Weibo hot topics results JSON file")
    parser.add_argument("--toutiao", type=Path, help="Toutiao hot topics results JSON file")
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
        log_input_summary(payloads)
        topic_groups, previous_titles, total_collected = process_articles(payloads, args.archive_dir)
        source_groups = group_by_source_types(
            [article for topic_articles in topic_groups.values() for article in topic_articles]
        )
        output = build_merged_output(payloads, source_groups, previous_titles, total_collected)
        total_after_domain_limits = output["output_stats"]["total_articles"]

        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(output, handle, ensure_ascii=False, indent=2)

        logger.info("✅ Merged and scored articles:")
        logger.info("   Input: %d articles", total_collected)
        logger.info("   Output: %d articles across %d source types", total_after_domain_limits, len(source_groups))
        logger.info("   File: %s", args.output)
        return 0
    except Exception as exc:
        logger.error("💥 Merge failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
