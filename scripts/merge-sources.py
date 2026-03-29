#!/usr/bin/env python3
"""
Merge data from enabled fetch steps with layered scoring and deduplication.

Reads output from fetch-rss.py, fetch-twitter.py, fetch-google.py,
fetch-github.py, fetch-github-trending.py, fetch-api.py, fetch-reddit.py,
fetch-v2ex.py, and any other compatible JSON inputs that are provided.
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
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover - defensive fallback
    fuzz = None


SCORING_CONFIG = {
    "fetch_rank_max": 3.0,
    "history_threshold": 0.88,
    "history_penalties": [
        (0.96, -16.0),
        (0.92, -12.0),
        (0.88, -8.0),
    ],
    "cross_source_hot_threshold": 0.86,
    "duplicate_threshold": 0.92,
    "cross_source_hot_per_extra_type": 2.0,
    "cross_source_hot_cap": 6.0,
    "recency_24h_bonus": 1.0,
    "recency_6h_bonus": 0.5,
    "topic_same_source_penalty": 1.5,
    "topic_same_domain_penalty": 0.75,
    "topic_first3_source_penalty": 3.0,
    "topic_first3_domain_penalty": 1.5,
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
    if source_type == "twitter":
        metrics = article.get("metrics", {})
        likes = metrics.get("like_count", 0)
        retweets = metrics.get("retweet_count", 0)
        replies = metrics.get("reply_count", 0)
        if likes >= 1000 or retweets >= 500 or replies >= 300:
            return float(SCORE_ENGAGEMENT_VIRAL)
        if likes >= 500 or retweets >= 200 or replies >= 150:
            return float(SCORE_ENGAGEMENT_HIGH)
        if likes >= 100 or retweets >= 50 or replies >= 60:
            return float(SCORE_ENGAGEMENT_MED)
        if likes >= 50 or retweets >= 20 or replies >= 20:
            return float(SCORE_ENGAGEMENT_LOW)
        return 0.0

    if source_type == "reddit":
        score = int(article.get("score", 0) or 0)
        comments = int(article.get("num_comments", 0) or 0)
        if score >= 1000 or comments >= 300:
            return float(SCORE_DISCUSSION_VIRAL)
        if score >= 500 or comments >= 150:
            return float(SCORE_DISCUSSION_HIGH)
        if score >= 200 or comments >= 80:
            return float(SCORE_DISCUSSION_MED)
        if score >= 100 or comments >= 30:
            return float(SCORE_DISCUSSION_LOW)
        return 0.0

    if source_type == "v2ex":
        return float(calculate_v2ex_replies_bonus(article.get("replies", 0)))

    return 0.0


def calculate_v2ex_replies_bonus(replies: Any) -> int:
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


def calculate_recency_bonus(article: Dict[str, Any]) -> float:
    article_date = parse_article_datetime(article.get("date"))
    if article_date is None:
        return 0.0

    hours_old = (datetime.now(timezone.utc) - article_date).total_seconds() / 3600
    if hours_old < 6:
        return SCORING_CONFIG["recency_24h_bonus"] + SCORING_CONFIG["recency_6h_bonus"]
    if hours_old < 24:
        return SCORING_CONFIG["recency_24h_bonus"]
    return 0.0


def initialize_article_scores(articles: List[Dict[str, Any]]) -> None:
    for article in articles:
        source_type = article.get("source_type", "")
        base_priority_score = float(normalize_priority(article.get("source_priority", 3)))
        local_extra_score = calculate_local_extra_score(article, source_type)
        article["score_breakdown"] = {
            "base_priority_score": base_priority_score,
            "local_extra_score": local_extra_score,
            "fetch_local_rank_score": 0.0,
            "history_penalty": 0.0,
            "cross_source_hot_bonus": 0.0,
            "recency_bonus": 0.0,
        }
        article["similarity_debug"] = {
            "best_history_similarity": 0.0,
            "duplicate_cluster_id": None,
            "cluster_size": 1,
            "cross_source_match_count": 0,
        }
        article["quality_score"] = base_priority_score
        article["final_score"] = base_priority_score


def assign_fetch_rank_scores(articles: List[Dict[str, Any]]) -> None:
    fetch_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for article in articles:
        fetch_groups[article.get("source_type", "unknown")].append(article)

    for fetch_type, group in fetch_groups.items():
        ordered = sorted(
            group,
            key=lambda item: (
                -(item["score_breakdown"]["base_priority_score"] + item["score_breakdown"]["local_extra_score"]),
                item.get("title", ""),
            ),
        )
        total = len(ordered)
        for rank, article in enumerate(ordered, start=1):
            if total <= 1:
                rank_pct = 1.0
            else:
                rank_pct = 1.0 - ((rank - 1) / (total - 1))
            article["score_breakdown"]["fetch_local_rank_score"] = round(
                SCORING_CONFIG["fetch_rank_max"] * rank_pct, 3
            )
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


def history_penalty_for_similarity(value: float) -> float:
    for threshold, penalty in SCORING_CONFIG["history_penalties"]:
        if value >= threshold:
            return penalty
    return 0.0


def apply_history_penalties(articles: List[Dict[str, Any]], previous_titles: Iterable[str]) -> None:
    previous_features = build_previous_title_features(previous_titles)
    for article in articles:
        similarity = best_history_similarity(article, previous_features)
        penalty = history_penalty_for_similarity(similarity)
        article["similarity_debug"]["best_history_similarity"] = round(similarity, 4)
        article["score_breakdown"]["history_penalty"] = penalty
        if penalty < 0:
            article["in_previous_digest"] = True


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


def apply_cross_source_hot_bonus(articles: List[Dict[str, Any]], pair_similarities: Dict[Tuple[int, int], float]) -> None:
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
        bonus = min(
            SCORING_CONFIG["cross_source_hot_cap"],
            SCORING_CONFIG["cross_source_hot_per_extra_type"] * extra_types,
        )
        for idx in indices:
            articles[idx]["score_breakdown"]["cross_source_hot_bonus"] = bonus
            articles[idx]["similarity_debug"]["cross_source_match_count"] = extra_types


def recalculate_final_scores(articles: List[Dict[str, Any]]) -> None:
    for article in articles:
        breakdown = article["score_breakdown"]
        breakdown["recency_bonus"] = calculate_recency_bonus(article)
        final_score = (
            breakdown["base_priority_score"]
            + breakdown["fetch_local_rank_score"]
            + breakdown["history_penalty"]
            + breakdown["cross_source_hot_bonus"]
            + breakdown["recency_bonus"]
        )
        article["final_score"] = round(final_score, 3)
        article["quality_score"] = article["final_score"]


def apply_similarity_scoring(articles: List[Dict[str, Any]], previous_titles: Iterable[str]) -> Dict[Tuple[int, int], float]:
    for article in articles:
        article["_similarity_features"] = build_similarity_features(article)

    assign_fetch_rank_scores(articles)
    apply_history_penalties(articles, previous_titles)

    pair_similarities: Dict[Tuple[int, int], float] = {}
    for i, j in build_candidate_pairs(articles):
        features_i = articles[i]["_similarity_features"]
        features_j = articles[j]["_similarity_features"]
        if not should_compare(features_i, features_j):
            continue
        pair_similarities[(i, j)] = calculate_similarity_from_features(features_i, features_j)

    apply_cross_source_hot_bonus(articles, pair_similarities)
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
    canonical["source_count"] = len(unique_sources)
    canonical["all_sources"] = unique_sources[:5]
    canonical["cluster_size"] = len(cluster_articles)
    canonical["similarity_debug"]["duplicate_cluster_id"] = cluster_id
    canonical["similarity_debug"]["cluster_size"] = len(cluster_articles)

    merged_topics = []
    seen_topics = set()
    for article in cluster_articles:
        for topic in article.get("topics", []):
            if topic not in seen_topics:
                seen_topics.add(topic)
                merged_topics.append(topic)
    canonical["topics"] = merged_topics or canonical.get("topics", [])
    return canonical


def deduplicate_articles(articles: List[Dict[str, Any]], previous_titles: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    if not articles:
        return articles

    for article in articles:
        article.setdefault("source_priority", normalize_priority(article.get("source_priority", article.get("priority", 3))))
    initialize_article_scores(articles)
    pair_similarities = apply_similarity_scoring(articles, previous_titles or [])

    duplicate_union = UnionFind(len(articles))
    for idx, article in enumerate(articles):
        norm_url = article["_similarity_features"]["normalized_url"]
        if norm_url:
            pass
        article["similarity_debug"]["duplicate_cluster_id"] = idx

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
                item.get("final_score", item.get("quality_score", 0)),
                item["score_breakdown"]["local_extra_score"],
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
        for file_path in sorted(archive_dir.rglob("*.json")):
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


def rerank_topic_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    remaining = list(articles)
    selected: List[Dict[str, Any]] = []
    source_counts: Dict[str, int] = defaultdict(int)
    domain_counts: Dict[str, int] = defaultdict(int)

    while remaining:
        slot = len(selected)
        source_penalty = (
            SCORING_CONFIG["topic_first3_source_penalty"]
            if slot < 3
            else SCORING_CONFIG["topic_same_source_penalty"]
        )
        domain_penalty = (
            SCORING_CONFIG["topic_first3_domain_penalty"]
            if slot < 3
            else SCORING_CONFIG["topic_same_domain_penalty"]
        )

        def display_score(article: Dict[str, Any]) -> Tuple[float, float]:
            source_type = article.get("source_type", "")
            domain = get_domain(article.get("link", ""))
            value = (
                article.get("final_score", article.get("quality_score", 0))
                - source_penalty * source_counts.get(source_type, 0)
                - domain_penalty * domain_counts.get(domain, 0)
            )
            return value, article.get("final_score", article.get("quality_score", 0))

        best = max(remaining, key=display_score)
        remaining.remove(best)
        selected.append(best)
        source_counts[best.get("source_type", "")] += 1
        domain = get_domain(best.get("link", ""))
        if domain:
            domain_counts[domain] += 1

    return selected


def group_by_topics(articles: List[Dict[str, Any]], dedup_across_topics: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    topic_groups: Dict[str, List[Dict[str, Any]]] = {}
    seen_article_ids: Set[str] = set()
    topic_priority = {
        "ai-models": 0,
        "ai-agents": 1,
        "ai-ecosystem": 2,
        "technology": 3,
        "developer-tools": 4,
        "markets-business": 5,
        "macro-policy": 6,
        "world-affairs": 7,
        "cybersecurity": 8,
        "github": 9,
        "trending": 13,
        "uncategorized": 99,
    }

    def get_topic_priority(topic: str) -> int:
        return topic_priority.get(topic, 99)

    for article in articles:
        topics = article.get("topics", []) or ["uncategorized"]
        sorted_topics = sorted(topics, key=get_topic_priority)
        article_id = normalize_title(article.get("title", ""))

        if dedup_across_topics and article_id in seen_article_ids:
            continue
        seen_article_ids.add(article_id)

        primary_topic = sorted_topics[0]
        topic_groups.setdefault(primary_topic, [])
        article_copy = article.copy()
        article_copy["primary_topic"] = primary_topic
        article_copy["all_topics"] = topics
        topic_groups[primary_topic].append(article_copy)

    for topic, topic_articles in topic_groups.items():
        ordered = sorted(topic_articles, key=lambda item: item.get("final_score", item.get("quality_score", 0)), reverse=True)
        topic_groups[topic] = rerank_topic_articles(ordered)

    return topic_groups


# ---------------------------------------------------------------------------
# Input normalization
# ---------------------------------------------------------------------------

def build_article(title: str, link: str, date: str, source_type: str, source_name: str, source_id: str, source_priority: int, topics: List[str], **extra: Any) -> Dict[str, Any]:
    article = {
        "title": title,
        "link": link,
        "date": date,
        "source_type": source_type,
        "source_name": source_name,
        "source_id": source_id,
        "source_priority": source_priority,
        "topics": topics,
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

    for source in reddit_data.get("subreddits", []):
        for article in source.get("articles", []):
            enriched = article.copy()
            enriched.update({
                "source_type": "reddit",
                "source_name": f"r/{source.get('subreddit', '')}",
                "source_id": source.get("source_id", ""),
                "source_priority": normalize_priority(source.get("priority", 3)),
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
                topics=repo.get("topics", []),
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
    }


def log_input_summary(payloads: Dict[str, Dict[str, Any]]) -> None:
    logging.info(
        "Loaded sources - RSS: %s, Twitter: %s, Google: %s, GitHub: %s + %s trending, Reddit: %s, API: %s, V2EX: %s",
        payloads["rss"].get("total_articles", 0),
        payloads["twitter"].get("total_articles", 0),
        payloads["google"].get("total_articles", 0),
        payloads["github"].get("total_articles", 0),
        payloads["trending"].get("total", 0),
        payloads["reddit"].get("total_posts", 0),
        payloads["api"].get("total_articles", 0),
        payloads["v2ex"].get("total_articles", 0),
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
    topic_groups: Dict[str, List[Dict[str, Any]]],
    previous_titles: List[str],
    total_collected: int,
) -> Dict[str, Any]:
    total_after_domain_limits = sum(len(items) for items in topic_groups.values())
    topic_counts = {topic: len(items) for topic, items in topic_groups.items()}
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
            "total_input": total_collected,
        },
        "processing": {
            "deduplication_applied": True,
            "multi_source_merging": True,
            "previous_hotspots_penalty": len(previous_titles) > 0,
            "quality_scoring": True,
            "scoring_version": "2.0",
        },
        "output_stats": {
            "total_articles": total_after_domain_limits,
            "topics_count": len(topic_groups),
            "topic_distribution": topic_counts,
        },
        "topics": {
            topic: {"count": len(items), "articles": items}
            for topic, items in topic_groups.items()
        },
    }


def main() -> int:
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
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path (default: auto-generated temp file)")
    parser.add_argument("--archive-dir", type=Path, help="Archive directory for previous hotspots penalty")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    logger = setup_logging(args.verbose)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-merged-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        payloads = load_input_payloads(args)
        log_input_summary(payloads)
        topic_groups, previous_titles, total_collected = process_articles(payloads, args.archive_dir)
        output = build_merged_output(payloads, topic_groups, previous_titles, total_collected)
        total_after_domain_limits = output["output_stats"]["total_articles"]

        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(output, handle, ensure_ascii=False, indent=2)

        logger.info("✅ Merged and scored articles:")
        logger.info("   Input: %d articles", total_collected)
        logger.info("   Output: %d articles across %d topics", total_after_domain_limits, len(topic_groups))
        logger.info("   File: %s", args.output)
        return 0
    except Exception as exc:
        logger.error("💥 Merge failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
