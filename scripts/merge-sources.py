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
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qsl, urlparse

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

SIMILARITY_LIMITS = {
    "max_word_bucket_size": 128,
    "max_cjk_bucket_size": 128,
    "max_history_word_bucket_size": 96,
    "max_history_cjk_bucket_size": 96,
    "max_history_compact_bucket_size": 96,
    "max_history_candidates": 48,
    "parallel_min_pairs": 5000,
    "parallel_min_cpus": 4,
    "batch_size": 512,
}

PAIR_SIMILARITY_MIN = min(
    SCORING_CONFIG["cross_source_hot_threshold"],
    SCORING_CONFIG["duplicate_threshold"],
)

SCORE_DEBUG_COMMENTS = {
    "scoring_debug": "评分计算说明，只保留 final_score 公式和各项分值。",
    "final_score": "merge-sources 阶段最终分，由多个分数组件相加得到。",
    "similarity_debug": "相似度与去重摘要，帮助理解历史重复、重复合并和跨来源共振。",
}

SCORE_COMPONENT_KEYS = (
    "base_priority_score",
    "fetch_local_rank_score",
    "history_score",
    "cross_source_hot_score",
    "recency_score",
    "local_extra_score",
)

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
RAW_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
COUNT_UNIT_RE = re.compile(r"\b\d+\s*(?:posts?|participants?|comments?|replies?)\b|\d+\s*(?:个帖子|位参与者|条评论|条回复|人参与)", re.IGNORECASE)
PROMOTION_SHAPE_RE = re.compile(r"(?:[¥￥$€£]\s*\d)|(?:\d+\s*(?:折|off))", re.IGNORECASE)
TRACKING_PARAM_PREFIXES = ("utm_", "spm", "scm", "mc_", "ref", "refer", "aff", "share", "tk")


@dataclass(frozen=True)
class MachineProfile:
    cpu_count: int
    memory_gb: float
    max_workers: int
    batch_size: int


def resolve_article_topic(article: Dict[str, Any], default: str = "") -> str:
    topic = str(article.get("topic") or "").strip()
    return topic or default


def resolve_cluster_topic(cluster_articles: List[Dict[str, Any]], default: str = "") -> str:
    topic_scores: Dict[str, float] = defaultdict(float)
    for article in cluster_articles:
        topic = resolve_article_topic(article)
        if not topic:
            continue
        topic_scores[topic] += float(article.get("final_score", 0) or 0)

    if topic_scores:
        return max(topic_scores.items(), key=lambda item: (item[1], item[0]))[0]

    for article in cluster_articles:
        topic = resolve_article_topic(article)
        if topic:
            return topic
    return default


def build_cluster_topic_candidates(cluster_articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    topic_scores: Dict[str, float] = defaultdict(float)
    topic_counts: Dict[str, int] = defaultdict(int)
    for article in cluster_articles:
        topic = resolve_article_topic(article)
        if not topic:
            continue
        topic_scores[topic] += float(article.get("final_score", 0) or 0)
        topic_counts[topic] += 1

    return [
        {
            "topic": topic,
            "score": round(topic_scores[topic], 3),
            "count": int(topic_counts[topic]),
        }
        for topic in sorted(topic_scores, key=lambda item: (-topic_scores[item], item))
    ]


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


def article_primary_link(article: Dict[str, Any]) -> str:
    return str(article.get("link") or article.get("reddit_url") or article.get("external_url") or "")


def article_display_title(article: Dict[str, Any]) -> str:
    return str(
        article.get("title")
        or article.get("name")
        or article.get("repo")
        or article_primary_link(article)
        or ""
    )


def summarize_cross_source_match(article: Dict[str, Any], similarity: float) -> Dict[str, Any]:
    return {
        "source_type": str(article.get("source_type") or ""),
        "source_name": str(article.get("source_name") or ""),
        "topic": str(article.get("topic") or ""),
        "title": article_display_title(article),
        "link": article_primary_link(article),
        "similarity": round(float(similarity or 0.0), 4),
    }


def cross_source_match_identity(match: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(match.get("source_type") or ""),
        str(match.get("title") or ""),
        str(match.get("link") or ""),
    )


def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def contains_cjk(text: str) -> bool:
    return bool(CJK_RE.search(text))


def detect_total_memory_gb() -> float:
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        phys_pages = os.sysconf("SC_PHYS_PAGES")
        if isinstance(page_size, int) and isinstance(phys_pages, int) and page_size > 0 and phys_pages > 0:
            return round((page_size * phys_pages) / (1024 ** 3), 2)
    except (AttributeError, OSError, ValueError):
        pass
    return 0.0


def detect_machine_profile() -> MachineProfile:
    cpu_count = max(1, int(os.cpu_count() or 1))
    memory_gb = detect_total_memory_gb()

    if cpu_count <= 2 or (memory_gb and memory_gb <= 4.5):
        return MachineProfile(
            cpu_count=cpu_count,
            memory_gb=memory_gb,
            max_workers=1,
            batch_size=128,
        )
    if cpu_count <= 4 or (memory_gb and memory_gb <= 8.0):
        return MachineProfile(
            cpu_count=cpu_count,
            memory_gb=memory_gb,
            max_workers=min(2, cpu_count),
            batch_size=256,
        )
    return MachineProfile(
        cpu_count=cpu_count,
        memory_gb=memory_gb,
        max_workers=min(4, cpu_count),
        batch_size=512,
    )


def similarity_bucket_limits(machine_profile: MachineProfile) -> Dict[str, int]:
    if machine_profile.cpu_count <= 2 or (machine_profile.memory_gb and machine_profile.memory_gb <= 4.5):
        return {
            "max_word_bucket_size": 48,
            "max_cjk_bucket_size": 64,
        }
    if machine_profile.cpu_count <= 4 or (machine_profile.memory_gb and machine_profile.memory_gb <= 8.0):
        return {
            "max_word_bucket_size": 80,
            "max_cjk_bucket_size": 96,
        }
    return {
        "max_word_bucket_size": SIMILARITY_LIMITS["max_word_bucket_size"],
        "max_cjk_bucket_size": SIMILARITY_LIMITS["max_cjk_bucket_size"],
    }


def count_tracking_params(url: str) -> int:
    try:
        parsed = urlparse(url or "")
    except Exception:
        return 0
    if not parsed.query:
        return 0
    total = 0
    for key, _ in parse_qsl(parsed.query, keep_blank_values=True):
        normalized = key.strip().lower()
        if any(normalized.startswith(prefix) for prefix in TRACKING_PARAM_PREFIXES):
            total += 1
    return total


def normalize_text(value: Any) -> str:
    return SPACE_RE.sub(" ", str(value or "").strip())


def extract_embedded_domains(text: str) -> Set[str]:
    domains: Set[str] = set()
    for match in RAW_URL_RE.findall(text or ""):
        domain = get_domain(match)
        if domain:
            domains.add(domain)
    return domains


def informative_token_count(text: str) -> int:
    return sum(1 for token in normalize_title(text).split() if len(token) >= 2)


def build_noise_signals(article: Dict[str, Any]) -> Dict[str, float]:
    title = normalize_text(article.get("title"))
    summary = normalize_text(article.get("summary") or article.get("snippet"))
    combined_text = normalize_text(" ".join(part for part in (title, summary) if part))
    link = str(article.get("link") or article.get("external_url") or article.get("reddit_url") or "")
    signals: Dict[str, float] = {}
    article_domain = get_domain(link)
    embedded_domains = extract_embedded_domains(combined_text)

    if RAW_URL_RE.search(combined_text):
        signals["embedded_url_text"] = 1.5
    if embedded_domains and article_domain and any(domain != article_domain for domain in embedded_domains):
        signals["embedded_external_redirect"] = 1.5
    if count_tracking_params(link) >= 1:
        signals["tracking_url"] = 1.5
    if len(COUNT_UNIT_RE.findall(combined_text)) >= 2:
        signals["engagement_chrome"] = 1.0
    if PROMOTION_SHAPE_RE.search(combined_text):
        signals["promotion_shape"] = 1.5
    if informative_token_count(title) <= 4 and len(title) <= 48:
        signals["short_low_information_title"] = 1.0
    return signals


def is_likely_promotional_noise(article: Dict[str, Any]) -> bool:
    signals = build_noise_signals(article)
    score = sum(signals.values())
    if score < 4.0:
        return False
    forum_wrapper_like = "engagement_chrome" in signals and any(
        key in signals for key in ("embedded_url_text", "embedded_external_redirect")
    )
    tracked_promo_like = "tracking_url" in signals and "promotion_shape" in signals
    compact_promo_like = "promotion_shape" in signals and "short_low_information_title" in signals and any(
        key in signals for key in ("embedded_url_text", "embedded_external_redirect", "tracking_url")
    )
    return forum_wrapper_like or tracked_promo_like or compact_promo_like


def filter_noise_articles(articles: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    removed = 0
    samples: List[Dict[str, Any]] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        if is_likely_promotional_noise(article):
            removed += 1
            if len(samples) < 5:
                samples.append(
                    {
                        "title": normalize_text(article.get("title"))[:160],
                        "link": str(article.get("link") or article.get("external_url") or article.get("reddit_url") or ""),
                        "signals": build_noise_signals(article),
                    }
                )
            continue
        kept.append(article)
    return kept, {
        "filtered_noise_articles": removed,
        "noise_filter_samples": samples,
    }


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
        "topic": resolve_article_topic(article, default="uncategorized") or "uncategorized",
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


def build_empty_score_components() -> Dict[str, float]:
    return {key: 0.0 for key in SCORE_COMPONENT_KEYS}


def build_empty_work_state() -> Dict[str, Any]:
    return {
        "score_components": build_empty_score_components(),
        "similarity_features": None,
    }


def ensure_work_state(article: Dict[str, Any]) -> Dict[str, Any]:
    raw_state = article.get("_work_state")
    if not isinstance(raw_state, dict):
        raw_state = build_empty_work_state()
    raw_score_components = raw_state.get("score_components")
    if not isinstance(raw_score_components, dict):
        raw_score_components = {}
    normalized_score_components = build_empty_score_components()
    for key in SCORE_COMPONENT_KEYS:
        normalized_score_components[key] = float(raw_score_components.get(key, 0.0) or 0.0)
    raw_state["score_components"] = normalized_score_components
    similarity_features = raw_state.get("similarity_features")
    raw_state["similarity_features"] = similarity_features if isinstance(similarity_features, dict) else None
    article["_work_state"] = raw_state
    return raw_state


def ensure_score_components(article: Dict[str, Any]) -> Dict[str, float]:
    work_state = ensure_work_state(article)
    return work_state["score_components"]


def ensure_similarity_features(article: Dict[str, Any]) -> Dict[str, Any]:
    work_state = ensure_work_state(article)
    similarity_features = work_state.get("similarity_features")
    if not isinstance(similarity_features, dict):
        similarity_features = build_similarity_features(article)
        work_state["similarity_features"] = similarity_features
    return similarity_features


def export_score_components(article: Dict[str, Any]) -> Dict[str, float]:
    score_components = article.get("score_components", {}) if isinstance(article.get("score_components"), dict) else {}
    return {
        key: float(score_components.get(key, 0.0) or 0.0)
        for key in SCORE_COMPONENT_KEYS
    }


def build_similarity_debug_state() -> Dict[str, Any]:
    return {
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


def ensure_similarity_debug(article: Dict[str, Any]) -> Dict[str, Any]:
    raw_debug = article.get("similarity_debug")
    if not isinstance(raw_debug, dict):
        raw_debug = build_similarity_debug_state()
    raw_debug.setdefault("_comment", SCORE_DEBUG_COMMENTS["similarity_debug"])
    raw_debug.setdefault("history_similarity", 0.0)
    raw_debug.setdefault("history_duplicate", False)
    duplicate_group = raw_debug.get("duplicate_group")
    if not isinstance(duplicate_group, dict):
        duplicate_group = {"merged": False, "cluster_size": 1}
    duplicate_group.setdefault("merged", False)
    duplicate_group.setdefault("cluster_size", 1)
    raw_debug["duplicate_group"] = duplicate_group
    cross_source_hot = raw_debug.get("cross_source_hot")
    if not isinstance(cross_source_hot, dict):
        cross_source_hot = {"matched_source_type_count": 0, "score": 0.0}
    cross_source_hot.setdefault("matched_source_type_count", 0)
    cross_source_hot.setdefault("score", 0.0)
    raw_debug["cross_source_hot"] = cross_source_hot
    raw_debug.setdefault("fields_comment_zh", build_similarity_debug_state()["fields_comment_zh"])
    article["similarity_debug"] = raw_debug
    return raw_debug


def build_scoring_debug_state() -> Dict[str, Any]:
    return {
        "_comment": SCORE_DEBUG_COMMENTS["scoring_debug"],
        "final_score_formula": "base_priority_score + fetch_local_rank_score + history_score + cross_source_hot_score + recency_score",
        "local_extra_score_note": "local_extra_score 仅作为站内热度参考信号输出，不直接计入 final_score。",
        "score_component_semantics": {
            "final_score_includes": [
                "base_priority_score",
                "fetch_local_rank_score",
                "history_score",
                "cross_source_hot_score",
                "recency_score",
            ],
            "reference_only": ["local_extra_score"],
            "note_zh": "final_score 只由 final_score_formula 中列出的 5 个分数组件相加得到；local_extra_score 仅用于解释站内热度，不参与 final_score 求和。",
        },
    }


def ensure_scoring_debug(article: Dict[str, Any]) -> Dict[str, Any]:
    raw_debug = article.get("scoring_debug")
    if not isinstance(raw_debug, dict):
        raw_debug = build_scoring_debug_state()
    defaults = build_scoring_debug_state()
    raw_debug.setdefault("_comment", SCORE_DEBUG_COMMENTS["scoring_debug"])
    raw_debug.setdefault("final_score_formula", defaults["final_score_formula"])
    raw_debug.setdefault("local_extra_score_note", defaults["local_extra_score_note"])
    raw_debug.setdefault("score_component_semantics", defaults["score_component_semantics"])
    article["scoring_debug"] = raw_debug
    return raw_debug


def set_history_similarity_debug(article: Dict[str, Any], *, similarity: float, is_duplicate: bool) -> None:
    similarity_debug = ensure_similarity_debug(article)
    similarity_debug["history_similarity"] = round(similarity, 4)
    similarity_debug["history_duplicate"] = bool(is_duplicate)


def set_cross_source_hot_debug(article: Dict[str, Any], *, matched_source_type_count: int, score: float) -> None:
    similarity_debug = ensure_similarity_debug(article)
    similarity_debug["cross_source_hot"]["matched_source_type_count"] = int(matched_source_type_count)
    similarity_debug["cross_source_hot"]["score"] = float(score or 0.0)


def set_duplicate_group_debug(article: Dict[str, Any], *, cluster_size: int) -> None:
    similarity_debug = ensure_similarity_debug(article)
    similarity_debug["duplicate_group"] = {
        "merged": cluster_size > 1,
        "cluster_size": int(cluster_size),
    }


def set_final_score_debug(article: Dict[str, Any], score_components: Dict[str, float]) -> None:
    scoring_debug = ensure_scoring_debug(article)
    scoring_debug["final_score"] = {
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
            "local_extra_score": "站内热度参考分，仅作解释字段；不参与 final_score 求和。",
            "fetch_local_rank_score": "同 source_type 内按排序位置得到的分值影响。",
            "history_score": "与历史热点相似时带来的分值影响，重复时通常为负分。",
            "cross_source_hot_score": "被多个不同 source_type 命中时的分值影响。",
            "recency_score": "时效性带来的分值影响。",
        },
        "component_membership": {
            "included_in_final_score": [
                "base_priority_score",
                "fetch_local_rank_score",
                "history_score",
                "cross_source_hot_score",
                "recency_score",
            ],
            "reference_only": ["local_extra_score"],
        },
    }


def set_article_final_score(article: Dict[str, Any], value: float) -> None:
    article["final_score"] = round(float(value or 0.0), 3)


def set_article_topic(article: Dict[str, Any], topic: str) -> None:
    normalized = str(topic or "").strip()
    article["topic"] = normalized
    article["primary_topic"] = normalized


def set_article_topic_candidates(article: Dict[str, Any], candidates: List[Dict[str, Any]]) -> None:
    if candidates:
        article["topic_candidates"] = candidates
    else:
        article.pop("topic_candidates", None)


def set_article_multi_source(article: Dict[str, Any], is_multi_source: bool) -> None:
    article["multi_source"] = bool(is_multi_source)


def set_article_cross_source_matches(article: Dict[str, Any], matches: List[Dict[str, Any]]) -> None:
    if matches:
        article["cross_source_matches"] = matches
    else:
        article.pop("cross_source_matches", None)


def normalize_article_source_priority(article: Dict[str, Any]) -> None:
    article["source_priority"] = normalize_priority(article.get("source_priority", article.get("priority", 3)))


def initialize_article_scores(articles: List[Dict[str, Any]]) -> None:
    for article in articles:
        source_type = article.get("source_type", "")
        base_priority_score = float(normalize_priority(article.get("source_priority", 3)))
        local_extra_details = calculate_local_extra_details(article, source_type)
        local_extra_score = local_extra_details["score"]
        score_components = build_empty_score_components()
        score_components["base_priority_score"] = base_priority_score
        score_components["local_extra_score"] = local_extra_score
        article["_work_state"] = {
            "score_components": score_components,
            "similarity_features": None,
        }
        article["similarity_debug"] = build_similarity_debug_state()
        article["scoring_debug"] = build_scoring_debug_state()
        set_article_final_score(article, base_priority_score)


def assign_fetch_rank_scores(articles: List[Dict[str, Any]]) -> None:
    fetch_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for article in articles:
        fetch_groups[article.get("source_type", "unknown")].append(article)

    for fetch_type, group in fetch_groups.items():
        ordered = sorted(
            group,
            key=lambda item: (
                -(ensure_score_components(item)["base_priority_score"] + ensure_score_components(item)["local_extra_score"]),
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
            ensure_score_components(article)["fetch_local_rank_score"] = rank_score
        logging.debug("Assigned fetch-local rank scores for %s (%d items)", fetch_type, total)


def build_previous_title_features(previous_titles: Iterable[str]) -> List[Dict[str, Any]]:
    return [
        {"raw": title, **build_similarity_features({"title": title, "link": ""})}
        for title in previous_titles
    ]


def build_previous_title_index(previous_titles: Iterable[str]) -> Dict[str, Any]:
    features = build_previous_title_features(previous_titles)
    word_buckets: Dict[str, List[int]] = defaultdict(list)
    cjk_buckets: Dict[str, List[int]] = defaultdict(list)
    compact_bigram_buckets: Dict[str, List[int]] = defaultdict(list)
    title_buckets: Dict[str, List[int]] = defaultdict(list)
    compact_buckets: Dict[str, List[int]] = defaultdict(list)

    for idx, feature in enumerate(features):
        for token in feature["word_tokens"]:
            word_buckets[token].append(idx)
        for token in feature["cjk_bigrams"]:
            cjk_buckets[token].append(idx)
        for token in feature["compact_bigrams"]:
            compact_bigram_buckets[token].append(idx)
        if feature["normalized_title"]:
            title_buckets[feature["normalized_title"]].append(idx)
        if feature["normalized_compact"]:
            compact_buckets[feature["normalized_compact"]].append(idx)

    return {
        "features": features,
        "word_buckets": word_buckets,
        "cjk_buckets": cjk_buckets,
        "compact_bigram_buckets": compact_bigram_buckets,
        "title_buckets": title_buckets,
        "compact_buckets": compact_buckets,
    }


def iter_history_candidate_indices(article_features: Dict[str, Any], previous_index: Dict[str, Any]) -> Iterable[int]:
    seen: Set[int] = set()

    for bucket_name, key in (
        ("title_buckets", article_features["normalized_title"]),
        ("compact_buckets", article_features["normalized_compact"]),
    ):
        if not key:
            continue
        for idx in previous_index[bucket_name].get(key, []):
            if idx not in seen:
                seen.add(idx)
                yield idx

    overlap_counts: Dict[int, int] = defaultdict(int)

    for token in article_features["word_tokens"]:
        bucket = previous_index["word_buckets"].get(token, [])
        if len(bucket) > SIMILARITY_LIMITS["max_history_word_bucket_size"]:
            continue
        for idx in bucket:
            if idx not in seen:
                overlap_counts[idx] += 3

    for token in article_features["cjk_bigrams"]:
        bucket = previous_index["cjk_buckets"].get(token, [])
        if len(bucket) > SIMILARITY_LIMITS["max_history_cjk_bucket_size"]:
            continue
        for idx in bucket:
            if idx not in seen:
                overlap_counts[idx] += 2

    for token in article_features["compact_bigrams"]:
        bucket = previous_index["compact_bigram_buckets"].get(token, [])
        if len(bucket) > SIMILARITY_LIMITS["max_history_compact_bucket_size"]:
            continue
        for idx in bucket:
            if idx not in seen:
                overlap_counts[idx] += 1

    ranked = sorted(overlap_counts.items(), key=lambda item: (-item[1], item[0]))
    for idx, _score in ranked[: SIMILARITY_LIMITS["max_history_candidates"]]:
        if idx not in seen:
            seen.add(idx)
            yield idx



def best_history_similarity(article: Dict[str, Any], previous_index: Dict[str, Any]) -> float:
    if not previous_index["features"]:
        return 0.0
    article_features = ensure_similarity_features(article)

    if article_features["normalized_title"] and previous_index["title_buckets"].get(article_features["normalized_title"]):
        return 1.0
    if article_features["normalized_compact"] and previous_index["compact_buckets"].get(article_features["normalized_compact"]):
        return 1.0

    best = 0.0
    features = previous_index["features"]
    for idx in iter_history_candidate_indices(article_features, previous_index):
        previous = features[idx]
        if not should_compare(article_features, previous):
            continue
        best = max(best, calculate_similarity_from_features(article_features, previous))
        if best >= 0.96:
            return best
    return best


def history_score_for_similarity(value: float) -> float:
    for threshold, score in SCORING_CONFIG["history_scores"]:
        if value >= threshold:
            return score
    return 0.0


def apply_history_scores(articles: List[Dict[str, Any]], previous_titles: Iterable[str]) -> None:
    started = time.perf_counter()
    previous_index = build_previous_title_index(previous_titles)
    logging.info("History similarity index: %d titles", len(previous_index["features"]))
    similarity_cache: Dict[Tuple[str, str, str], float] = {}
    for article in articles:
        features = ensure_similarity_features(article)
        cache_key = (
            features.get("normalized_title", ""),
            features.get("normalized_compact", ""),
            features.get("normalized_url", ""),
        )
        similarity = similarity_cache.get(cache_key)
        if similarity is None:
            similarity = best_history_similarity(article, previous_index)
            similarity_cache[cache_key] = similarity
        score = history_score_for_similarity(similarity)
        ensure_score_components(article)["history_score"] = score
        set_history_similarity_debug(article, similarity=similarity, is_duplicate=score < 0)
    logging.info(
        "History similarity scoring finished in %.3fs (cache=%d)",
        time.perf_counter() - started,
        len(similarity_cache),
    )


def build_candidate_pairs(
    features_list: Sequence[Dict[str, Any]],
    machine_profile: Optional[MachineProfile] = None,
) -> Iterable[Tuple[int, int]]:
    bucket_limits = similarity_bucket_limits(machine_profile or detect_machine_profile())
    yielded: Set[Tuple[int, int]] = set()
    skipped_word_buckets = 0
    skipped_cjk_buckets = 0

    word_buckets: Dict[str, List[int]] = defaultdict(list)
    cjk_buckets: Dict[str, List[int]] = defaultdict(list)
    url_buckets: Dict[str, List[int]] = defaultdict(list)
    title_buckets: Dict[str, List[int]] = defaultdict(list)
    compact_buckets: Dict[str, List[int]] = defaultdict(list)

    for idx, features in enumerate(features_list):
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
        if len(bucket) > bucket_limits["max_word_bucket_size"]:
            skipped_word_buckets += 1
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                pair = (bucket[i], bucket[j])
                pair_counts[pair] += 1

    cjk_counts: Dict[Tuple[int, int], int] = defaultdict(int)
    for bucket in cjk_buckets.values():
        if len(bucket) < 2:
            continue
        if len(bucket) > bucket_limits["max_cjk_bucket_size"]:
            skipped_cjk_buckets += 1
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                pair = (bucket[i], bucket[j])
                cjk_counts[pair] += 1

    for pair, count in pair_counts.items():
        if count >= 2 and pair not in yielded:
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

    if skipped_word_buckets or skipped_cjk_buckets:
        logging.info(
            "Similarity bucket guard skipped %d word buckets and %d cjk buckets",
            skipped_word_buckets,
            skipped_cjk_buckets,
        )


def exact_similarity_hint(features_a: Dict[str, Any], features_b: Dict[str, Any]) -> Optional[float]:
    if features_a["normalized_title"] and features_a["normalized_title"] == features_b["normalized_title"]:
        return 1.0
    if features_a["normalized_compact"] and features_a["normalized_compact"] == features_b["normalized_compact"]:
        return 1.0
    if features_a["normalized_url"] and features_a["normalized_url"] == features_b["normalized_url"]:
        return 1.0
    return None


def build_similarity_aggregation(article_count: int) -> Dict[str, Any]:
    return {
        "duplicate_union": UnionFind(article_count),
        "hot_union": UnionFind(article_count),
        "matches_by_index": defaultdict(list),
        "seen_match_keys": defaultdict(set),
        "kept_pairs": 0,
    }


def consume_similarity_pair(
    articles: List[Dict[str, Any]],
    features_list: Sequence[Dict[str, Any]],
    aggregation: Dict[str, Any],
    i: int,
    j: int,
    similarity: float,
) -> None:
    if similarity < PAIR_SIMILARITY_MIN:
        return

    aggregation["kept_pairs"] += 1
    same_source = articles[i].get("source_type") == articles[j].get("source_type")
    duplicate_union: UnionFind = aggregation["duplicate_union"]
    hot_union: UnionFind = aggregation["hot_union"]
    matches_by_index: Dict[int, List[Dict[str, Any]]] = aggregation["matches_by_index"]
    seen_match_keys: Dict[int, Set[Tuple[str, str, str]]] = aggregation["seen_match_keys"]

    same_normalized_url = (
        bool(features_list[i].get("normalized_url"))
        and features_list[i].get("normalized_url") == features_list[j].get("normalized_url")
    )
    if similarity >= SCORING_CONFIG["duplicate_threshold"] or same_normalized_url:
        duplicate_union.union(i, j)

    if similarity < SCORING_CONFIG["cross_source_hot_threshold"] or same_source:
        return

    hot_union.union(i, j)

    match_j = summarize_cross_source_match(articles[j], similarity)
    match_i = summarize_cross_source_match(articles[i], similarity)
    key_j = cross_source_match_identity(match_j)
    key_i = cross_source_match_identity(match_i)

    if key_j not in seen_match_keys[i]:
        matches_by_index[i].append(match_j)
        seen_match_keys[i].add(key_j)
    if key_i not in seen_match_keys[j]:
        matches_by_index[j].append(match_i)
        seen_match_keys[j].add(key_i)



def finalize_similarity_aggregation(articles: List[Dict[str, Any]], aggregation: Dict[str, Any]) -> None:
    hot_union: UnionFind = aggregation["hot_union"]
    matches_by_index: Dict[int, List[Dict[str, Any]]] = aggregation["matches_by_index"]

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
            ensure_score_components(articles[idx])["cross_source_hot_score"] = score
            set_cross_source_hot_debug(articles[idx], matched_source_type_count=extra_types, score=score)

    for idx, matches in list(matches_by_index.items()):
        matches_by_index[idx] = sorted(
            matches,
            key=lambda item: (
                -float(item.get("similarity", 0) or 0),
                str(item.get("source_type", "") or ""),
                str(item.get("title", "") or ""),
            ),
        )

    aggregation.pop("seen_match_keys", None)


def merge_cross_source_matches(cluster_indices: List[int], matches_by_index: Dict[int, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    merged_matches: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str, str]] = set()
    for idx in cluster_indices:
        for match in matches_by_index.get(idx, []):
            identity = cross_source_match_identity(match)
            if identity in seen:
                continue
            seen.add(identity)
            merged_matches.append(match)
    return sorted(
        merged_matches,
        key=lambda item: (
            -float(item.get("similarity", 0) or 0),
            str(item.get("source_type", "") or ""),
            str(item.get("title", "") or ""),
        ),
    )


def recalculate_final_scores(articles: List[Dict[str, Any]]) -> None:
    for article in articles:
        score_components = ensure_score_components(article)
        recency_details = calculate_recency_score_details(article)
        score_components["recency_score"] = recency_details["score"]
        final_score = (
            score_components["base_priority_score"]
            + score_components["fetch_local_rank_score"]
            + score_components["history_score"]
            + score_components["cross_source_hot_score"]
            + score_components["recency_score"]
        )
        set_article_final_score(article, final_score)
        set_final_score_debug(article, score_components)
        set_cross_source_hot_debug(
            article,
            matched_source_type_count=int(ensure_similarity_debug(article)["cross_source_hot"].get("matched_source_type_count", 0) or 0),
            score=score_components["cross_source_hot_score"],
        )


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


def _compute_pair_similarity(args: Tuple[int, int, Dict[str, Any], Dict[str, Any], Optional[float]]) -> Tuple[Tuple[int, int], float]:
    (i, j, features_i, features_j, hinted_similarity) = args
    similarity = hinted_similarity if hinted_similarity is not None else calculate_similarity_from_features(features_i, features_j)
    return ((i, j), similarity)


def _compute_pair_similarity_batch(
    batch: List[Tuple[int, int, Dict[str, Any], Dict[str, Any], Optional[float]]]
) -> List[Tuple[Tuple[int, int], float]]:
    results: List[Tuple[Tuple[int, int], float]] = []
    for pair in batch:
        pair_key, similarity = _compute_pair_similarity(pair)
        if similarity >= PAIR_SIMILARITY_MIN:
            results.append((pair_key, similarity))
    return results


def iter_similarity_tasks(
    features_list: Sequence[Dict[str, Any]],
    machine_profile: Optional[MachineProfile] = None,
) -> Iterable[Tuple[int, int, Dict[str, Any], Dict[str, Any], Optional[float]]]:
    for i, j in build_candidate_pairs(features_list, machine_profile=machine_profile):
        features_i = features_list[i]
        features_j = features_list[j]
        if not should_compare(features_i, features_j):
            continue
        yield (i, j, features_i, features_j, exact_similarity_hint(features_i, features_j))


def batched(
    values: Iterable[Tuple[int, int, Dict[str, Any], Dict[str, Any], Optional[float]]],
    size: int,
) -> Iterable[List[Tuple[int, int, Dict[str, Any], Dict[str, Any], Optional[float]]]]:
    batch: List[Tuple[int, int, Dict[str, Any], Dict[str, Any], Optional[float]]] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def apply_similarity_scoring(articles: List[Dict[str, Any]], previous_titles: Iterable[str]) -> Dict[str, Any]:
    previous_titles = list(previous_titles)
    assign_fetch_rank_scores(articles)
    apply_history_scores(articles, previous_titles)
    machine_profile = detect_machine_profile()
    features_list = [ensure_similarity_features(article) for article in articles]
    parallel_eligible = machine_profile.max_workers > 1 and machine_profile.cpu_count >= SIMILARITY_LIMITS["parallel_min_cpus"]

    candidate_started = time.perf_counter()
    aggregation = build_similarity_aggregation(len(articles))
    if parallel_eligible:
        tasks = list(iter_similarity_tasks(features_list, machine_profile=machine_profile))
        candidate_elapsed = time.perf_counter() - candidate_started
        logging.info(
            "Similarity scoring: %d articles, %d history titles, %d candidate pairs built in %.3fs (cpu=%d, mem=%.2fGB, workers=%d)",
            len(articles),
            len(previous_titles),
            len(tasks),
            candidate_elapsed,
            machine_profile.cpu_count,
            machine_profile.memory_gb,
            machine_profile.max_workers,
        )
        use_parallel = len(tasks) >= SIMILARITY_LIMITS["parallel_min_pairs"]
    else:
        tasks = []
        use_parallel = False
        candidate_elapsed = 0.0

    similarity_started = time.perf_counter()

    if use_parallel:
        with ThreadPoolExecutor(max_workers=machine_profile.max_workers) as executor:
            futures = [
                executor.submit(_compute_pair_similarity_batch, batch)
                for batch in batched(tasks, machine_profile.batch_size)
            ]
            for future in as_completed(futures):
                for (i, j), sim in future.result():
                    consume_similarity_pair(articles, features_list, aggregation, i, j, sim)
    else:
        candidate_count = 0
        for pair in iter_similarity_tasks(features_list, machine_profile=machine_profile):
            candidate_count += 1
            (i, j), sim = _compute_pair_similarity(pair)
            consume_similarity_pair(articles, features_list, aggregation, i, j, sim)
        candidate_elapsed = time.perf_counter() - candidate_started
        logging.info(
            "Similarity scoring: %d articles, %d history titles, %d candidate pairs built in %.3fs (cpu=%d, mem=%.2fGB, workers=%d, mode=streaming)",
            len(articles),
            len(previous_titles),
            candidate_count,
            candidate_elapsed,
            machine_profile.cpu_count,
            machine_profile.memory_gb,
            machine_profile.max_workers,
        )

    finalize_similarity_aggregation(articles, aggregation)
    logging.info(
        "Similarity calculation finished in %.3fs, kept %d pairs >= %.2f",
        time.perf_counter() - similarity_started,
        int(aggregation.get("kept_pairs", 0) or 0),
        PAIR_SIMILARITY_MIN,
    )

    recalculate_final_scores(articles)
    return aggregation


def merge_cluster_metadata(
    canonical: Dict[str, Any],
    cluster_articles: List[Dict[str, Any]],
    cross_source_matches: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    set_article_multi_source(canonical, len({a.get("source_type") for a in cluster_articles}) > 1)
    set_duplicate_group_debug(canonical, cluster_size=len(cluster_articles))

    canonical_identity = (
        str(canonical.get("source_type") or ""),
        article_display_title(canonical),
        article_primary_link(canonical),
    )
    filtered_cross_source_matches = [
        match for match in list(cross_source_matches or [])
        if cross_source_match_identity(match) != canonical_identity
    ]
    set_article_cross_source_matches(canonical, filtered_cross_source_matches)

    merged_topic = resolve_cluster_topic(cluster_articles, default=resolve_article_topic(canonical, default=""))
    if merged_topic:
        set_article_topic(canonical, merged_topic)
    set_article_topic_candidates(canonical, build_cluster_topic_candidates(cluster_articles))

    return canonical


def filter_historical_exact_duplicates(
    articles: List[Dict[str, Any]],
    previous_hotspots: Optional[Dict[str, List[str]]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    previous_title_keys = {
        normalize_title(title)
        for title in ((previous_hotspots or {}).get("titles") or [])
        if str(title or "").strip()
    }
    previous_link_keys = {
        normalize_url(link)
        for link in ((previous_hotspots or {}).get("links") or [])
        if str(link or "").strip()
    }
    if not previous_title_keys and not previous_link_keys:
        return articles, 0

    kept: List[Dict[str, Any]] = []
    removed = 0
    for article in articles:
        title_key = normalize_title(article_display_title(article))
        link_key = normalize_url(article_primary_link(article))
        if (title_key and title_key in previous_title_keys) or (link_key and link_key in previous_link_keys):
            removed += 1
            continue
        kept.append(article)
    return kept, removed


def deduplicate_articles(articles: List[Dict[str, Any]], previous_hotspots: Optional[Dict[str, List[str]]] = None) -> List[Dict[str, Any]]:
    if not articles:
        return articles

    exact_filtered_articles, historical_exact_removed = filter_historical_exact_duplicates(articles, previous_hotspots)
    if historical_exact_removed:
        logging.info("Historical exact dedup removed %d articles before similarity stage", historical_exact_removed)

    previous_titles = ((previous_hotspots or {}).get("titles") or [])
    working_articles = build_working_articles(exact_filtered_articles)
    similarity_aggregation = apply_similarity_scoring(working_articles, previous_titles)
    cross_source_matches_by_index = similarity_aggregation.get("matches_by_index", {})
    duplicate_union: UnionFind = similarity_aggregation["duplicate_union"]

    clusters: Dict[int, List[int]] = defaultdict(list)
    for idx, _article in enumerate(working_articles):
        clusters[duplicate_union.find(idx)].append(idx)

    deduplicated = []
    for cluster_indices in clusters.values():
        cluster_articles = [working_articles[idx] for idx in cluster_indices]
        canonical = max(
            cluster_articles,
            key=lambda item: (
                item.get("final_score", 0),
                ensure_score_components(item)["local_extra_score"],
                item.get("title", ""),
            ),
        )
        canonical = merge_cluster_metadata(
            canonical,
            cluster_articles,
            cross_source_matches=merge_cross_source_matches(cluster_indices, cross_source_matches_by_index),
        )
        deduplicated.append(canonical)

    for article in deduplicated:
        article["score_components"] = {
            key: float(ensure_score_components(article).get(key, 0.0) or 0.0)
            for key in SCORE_COMPONENT_KEYS
        }
        article.pop("_work_state", None)

    deduplicated.sort(key=lambda item: item.get("final_score", 0), reverse=True)
    logging.info("Deduplication: %d → %d articles", len(working_articles), len(deduplicated))
    return deduplicated


def load_previous_hotspots(archive_dir: Path) -> Dict[str, List[str]]:
    if not archive_dir.exists():
        return {"titles": [], "links": []}

    seen_titles: List[str] = []
    seen_links: List[str] = []
    try:
        for file_path in sorted(archive_dir.rglob("daily*.json")):
            if file_path.parent.name != "json":
                continue
            match = re.search(r"(\d{4}-\d{2}-\d{2})", str(file_path))
            if match:
                try:
                    file_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                    if file_date >= local_now().date():
                        continue
                except ValueError:
                    continue
            with open(file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            for topic in data.get("topics", []):
                for item in topic.get("items", []):
                    title = str(item.get("title", "")).strip()
                    link = str(item.get("link", "")).strip()
                    if title:
                        seen_titles.append(title)
                    if link:
                        seen_links.append(link)
    except Exception as exc:
        logging.debug("Failed to load previous hotspots: %s", exc)

    logging.info(
        "Loaded %d titles and %d links from previous archive under %s",
        len(seen_titles),
        len(seen_links),
        archive_dir,
    )
    return {"titles": seen_titles, "links": seen_links}


def sanitize_article_record(article: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = article.copy()
    sanitized.pop("_work_state", None)
    sanitized.pop("_similarity_features", None)
    sanitized.pop("_score_components", None)
    return sanitized


def build_working_articles(articles: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    working_articles: List[Dict[str, Any]] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        working_article = sanitize_article_record(article)
        normalize_article_source_priority(working_article)
        working_articles.append(working_article)
    initialize_article_scores(working_articles)
    return working_articles


def group_by_source_types(articles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    source_groups: Dict[str, List[Dict[str, Any]]] = {}
    for article in articles:
        source_type = str(article.get("source_type", "") or "unknown")
        source_groups.setdefault(source_type, []).append(sanitize_article_record(article))

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
        normalize_article_source_priority(normalized)
        set_article_topic(normalized, resolve_article_topic(normalized, default="uncategorized") or "uncategorized")
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
            article
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


def build_processing_summary(previous_hotspots: Dict[str, List[str]], total_collected: int) -> Dict[str, Any]:
    previous_titles = previous_hotspots.get("titles") or []
    previous_links = previous_hotspots.get("links") or []
    return {
        "deduplication_applied": True,
        "noise_filter_applied": True,
        "previous_hotspots_scoring_applied": len(previous_titles) > 0,
        "previous_hotspots_exact_link_index_applied": len(previous_links) > 0,
        "scoring_applied": True,
        "scoring_version": "2.0",
        "score_formula": "base_priority_score + fetch_local_rank_score + history_score + cross_source_hot_score + recency_score",
        "scoring_config": build_output_scoring_config(),
        "input_total_articles": total_collected,
    }


def project_article_output(article: Dict[str, Any]) -> Dict[str, Any]:
    projected_source = sanitize_article_record(article)
    output = {
        "title": projected_source.get("title"),
        "link": projected_source.get("link"),
        "date": projected_source.get("date"),
        "primary_topic": projected_source.get("primary_topic") or projected_source.get("topic"),
        "source_type": projected_source.get("source_type"),
        "source_id": projected_source.get("source_id"),
        "source_name": projected_source.get("source_name"),
        "source_priority": projected_source.get("source_priority"),
        "final_score": projected_source.get("final_score"),
        "score_components": export_score_components(article),
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
        "google_query",
        "twitter_query",
        "reddit_query",
        "name",
        "repo",
        "published_at",
        "cross_source_matches",
        "topic_candidates",
    )
    for field in optional_fields:
        if field in projected_source:
            output[field] = projected_source.get(field)
    return output


def build_merged_output(
    payloads: Dict[str, Dict[str, Any]],
    source_groups: Dict[str, List[Dict[str, Any]]],
    previous_hotspots: Dict[str, List[str]],
    total_collected: int,
    noise_report: Optional[Dict[str, Any]] = None,
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
            **build_processing_summary(previous_hotspots, total_collected),
            **(noise_report or {}),
        },
        "source_types": {
            source_type: {
                "count": len(items),
                "articles": [project_article_output(article) for article in items],
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
        previous_hotspots: Dict[str, List[str]] = load_previous_hotspots(args.archive_dir) if args.archive_dir else {"titles": [], "links": []}
        collected_articles = collect_articles(payloads)
        filtered_articles, noise_report = filter_noise_articles(collected_articles)
        if noise_report.get("filtered_noise_articles", 0):
            logger.info("Noise filter removed %d low-signal promotional articles", noise_report["filtered_noise_articles"])
        deduplicated_articles = deduplicate_articles(filtered_articles, previous_hotspots)
        source_groups = group_by_source_types(deduplicated_articles)
        output = build_merged_output(payloads, source_groups, previous_hotspots, total_collected, noise_report=noise_report)

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
