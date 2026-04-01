#!/usr/bin/env python3
"""
配置校验脚本。

职责：
- 校验 split config 体系下的 defaults 与 workspace overlay
- 检查字段类型、必填项和未知字段
- 在运行前尽早暴露配置错误

执行逻辑：
1. 分别加载 rss / twitter / github / reddit / api / topics / runtime 配置
2. 对各配置执行结构校验与字段校验
3. 输出校验结果并在错误时返回非零退出码

输入文件职责：
- `config/defaults/*.json`
  默认配置基线
- `<WORKSPACE>/config/news-hotspots-*.json`
  workspace overlay，可选存在
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from config_loader import (
        load_merged_api_sources,
        load_merged_github_sources,
        load_merged_reddit_sources,
        load_merged_rss_sources,
        load_merged_runtime_config,
        load_merged_topics,
        load_merged_twitter_sources,
    )
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import (
        load_merged_api_sources,
        load_merged_github_sources,
        load_merged_reddit_sources,
        load_merged_rss_sources,
        load_merged_runtime_config,
        load_merged_topics,
        load_merged_twitter_sources,
    )


SOURCE_FILES = {
    "rss": ("rss.json", "news-hotspots-rss.json", "url"),
    "twitter": ("twitter.json", "news-hotspots-twitter.json", "handle"),
    "github": ("github.json", "news-hotspots-github.json", "repo"),
    "reddit": ("reddit.json", "news-hotspots-reddit.json", "subreddit"),
}


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


def load_json_file(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def require_object_list(data: Dict[str, Any], key: str, label: str) -> List[Dict[str, Any]]:
    items = data.get(key)
    if not isinstance(items, list):
        raise ValueError(f"{label}: expected '{key}' to be a list")
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError(f"{label}: entries under '{key}' must be objects")
        normalized.append(item)
    return normalized


def validate_source_file(data: Dict[str, Any], source_type: str, required_field: str, valid_topics: Iterable[str]) -> List[str]:
    errors: List[str] = []
    valid_topics_set = set(valid_topics)
    sources = require_object_list(data, "sources", f"{source_type}.json")
    ids: List[str] = []
    for source in sources:
        source_id = str(source.get("id") or "").strip()
        ids.append(source_id)
        if not source_id:
            errors.append(f"{source_type}.json: source id is required")
        if source.get("type") != source_type:
            errors.append(f"{source_type}.json: source '{source_id}' must have type='{source_type}'")
        if not str(source.get("name") or "").strip():
            errors.append(f"{source_type}.json: source '{source_id}' missing name")
        if not isinstance(source.get("enabled"), bool):
            errors.append(f"{source_type}.json: source '{source_id}' missing enabled boolean")
        priority = source.get("priority")
        if not isinstance(priority, int) or not (1 <= priority <= 10):
            errors.append(f"{source_type}.json: source '{source_id}' priority must be 1-10")
        topic = str(source.get("topic") or "").strip()
        if not topic:
            errors.append(f"{source_type}.json: source '{source_id}' missing topic")
        elif topic not in valid_topics_set:
            errors.append(f"{source_type}.json: source '{source_id}' references invalid topic '{topic}'")
        if not str(source.get(required_field) or "").strip():
            errors.append(f"{source_type}.json: source '{source_id}' missing '{required_field}'")
    duplicates = {item for item in ids if item and ids.count(item) > 1}
    if duplicates:
        errors.append(f"{source_type}.json: duplicate ids {sorted(duplicates)}")
    return errors


def validate_topics(data: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    topics = require_object_list(data, "topics", "topics.json")
    ids: List[str] = []
    for topic in topics:
        topic_id = str(topic.get("id") or "").strip()
        ids.append(topic_id)
        if not topic_id:
            errors.append("topics.json: topic id is required")
        for field in ("emoji", "label", "description"):
            if not str(topic.get(field) or "").strip():
                errors.append(f"topics.json: topic '{topic_id}' missing {field}")
        search = topic.get("search")
        if not isinstance(search, dict):
            errors.append(f"topics.json: topic '{topic_id}' missing search object")
        else:
            for field in ("google_queries", "twitter_queries", "reddit_queries", "github_queries", "exclude"):
                if field in search and not isinstance(search.get(field), list):
                    errors.append(f"topics.json: topic '{topic_id}' search.{field} must be a list")
        display = topic.get("display")
        if not isinstance(display, dict):
            errors.append(f"topics.json: topic '{topic_id}' missing display object")
        elif not isinstance(display.get("max_items"), int) or display.get("max_items", 0) < 1:
            errors.append(f"topics.json: topic '{topic_id}' display.max_items must be >= 1")
    duplicates = {item for item in ids if item and ids.count(item) > 1}
    if duplicates:
        errors.append(f"topics.json: duplicate topic ids {sorted(duplicates)}")
    return errors


def validate_api_sources(data: Dict[str, Any], valid_topics: Iterable[str]) -> List[str]:
    errors: List[str] = []
    valid_topics_set = set(valid_topics)
    sources = require_object_list(data, "sources", "api.json")
    ids: List[str] = []
    for source in sources:
        source_id = str(source.get("id") or "").strip()
        ids.append(source_id)
        if not source_id:
            errors.append("api.json: source id is required")
        if not str(source.get("name") or "").strip():
            errors.append(f"api.json: source '{source_id}' missing name")
        if not isinstance(source.get("enabled"), bool):
            errors.append(f"api.json: source '{source_id}' missing enabled boolean")
        priority = source.get("priority")
        if not isinstance(priority, int) or not (1 <= priority <= 10):
            errors.append(f"api.json: source '{source_id}' priority must be 1-10")
        topic = str(source.get("topic") or "").strip()
        if not topic:
            errors.append(f"api.json: source '{source_id}' missing topic")
        elif topic not in valid_topics_set:
            errors.append(f"api.json: source '{source_id}' references invalid topic '{topic}'")
    duplicates = {item for item in ids if item and ids.count(item) > 1}
    if duplicates:
        errors.append(f"api.json: duplicate ids {sorted(duplicates)}")
    return errors


def validate_runtime(data: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required_fetch = {
        "rss": {"request_timeout_s", "max_workers", "max_articles_per_feed", "retry_count", "retry_delay_s", "cache_ttl_hours"},
        "github": {"request_timeout_s", "cooldown_s", "releases_per_repo", "retry_count", "retry_delay_s", "cache_ttl_hours"},
        "github_trending": {"request_timeout_s", "cooldown_s", "min_stars", "per_topic"},
        "google": {"request_timeout_s", "cooldown_s", "results_per_query"},
        "twitter": {"request_timeout_s", "cooldown_s", "count", "results_per_query"},
        "reddit": {"request_timeout_s", "cooldown_s", "results_per_query"},
        "v2ex": {"request_timeout_s", "cooldown_s"},
        "zhihu": {"request_timeout_s", "cooldown_s", "limit"},
        "weibo": {"request_timeout_s", "cooldown_s", "limit"},
        "toutiao": {"request_timeout_s", "cooldown_s", "limit"},
        "api": {"request_timeout_s", "max_workers", "limit", "host_cooldowns"},
    }
    pipeline = data.get("pipeline")
    if not isinstance(pipeline, dict):
        return ["runtime.json: missing pipeline object"]
    for field in ("fetch_step_timeout_s", "merge_timeout_s", "hotspots_timeout_s", "default_hotspots_top_n", "archive_retention_days"):
        if not isinstance(pipeline.get(field), int) or pipeline.get(field, 0) < 1:
            errors.append(f"runtime.json: pipeline.{field} must be integer >= 1")

    fetch = data.get("fetch")
    if not isinstance(fetch, dict):
        return errors + ["runtime.json: missing fetch object"]
    for source_type, fields in required_fetch.items():
        config = fetch.get(source_type)
        if not isinstance(config, dict):
            errors.append(f"runtime.json: missing fetch.{source_type} object")
            continue
        missing = sorted(field for field in fields if field not in config)
        if missing:
            errors.append(f"runtime.json: fetch.{source_type} missing fields {missing}")

    diagnostics = data.get("diagnostics")
    if not isinstance(diagnostics, dict):
        errors.append("runtime.json: missing diagnostics object")
    else:
        for field in ("history_days", "report_limit", "error_text_limit"):
            if not isinstance(diagnostics.get(field), int) or diagnostics.get(field, 0) < 1:
                errors.append(f"runtime.json: diagnostics.{field} must be integer >= 1")
        if not isinstance(diagnostics.get("degraded_threshold"), (int, float)):
            errors.append("runtime.json: diagnostics.degraded_threshold must be number")
        thresholds = diagnostics.get("slow_request_thresholds_s")
        if not isinstance(thresholds, list) or not thresholds or not all(isinstance(item, (int, float)) and item > 0 for item in thresholds):
            errors.append("runtime.json: diagnostics.slow_request_thresholds_s must be non-empty positive number list")

    cache = data.get("cache")
    if not isinstance(cache, dict):
        errors.append("runtime.json: missing cache object")
    else:
        for field in ("rss_cache_path", "github_cache_path"):
            if not str(cache.get(field) or "").strip():
                errors.append(f"runtime.json: cache.{field} is required")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate split config files for news-hotspots.")
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"))
    parser.add_argument("--config", type=Path, default=Path("workspace/config"))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.verbose)
    config_dir = args.config if args.config.exists() else None
    errors: List[str] = []

    topics_data = load_json_file(args.defaults / "topics.json")
    errors.extend(validate_topics(topics_data))
    topic_ids = [topic["id"] for topic in topics_data.get("topics", []) if isinstance(topic, dict) and topic.get("id")]

    for source_type, (defaults_name, _, required_field) in SOURCE_FILES.items():
        defaults_data = load_json_file(args.defaults / defaults_name)
        errors.extend(validate_source_file(defaults_data, source_type, required_field, topic_ids))

    api_defaults = load_json_file(args.defaults / "api.json")
    errors.extend(validate_api_sources(api_defaults, topic_ids))

    runtime_defaults = load_json_file(args.defaults / "runtime.json")
    errors.extend(validate_runtime(runtime_defaults))

    if config_dir:
        overlay_checks = {
            "news-hotspots-rss.json": lambda path: validate_source_file(load_json_file(path), "rss", "url", topic_ids),
            "news-hotspots-twitter.json": lambda path: validate_source_file(load_json_file(path), "twitter", "handle", topic_ids),
            "news-hotspots-github.json": lambda path: validate_source_file(load_json_file(path), "github", "repo", topic_ids),
            "news-hotspots-reddit.json": lambda path: validate_source_file(load_json_file(path), "reddit", "subreddit", topic_ids),
            "news-hotspots-topics.json": lambda path: validate_topics(load_json_file(path)),
            "news-hotspots-api.json": lambda path: validate_api_sources(load_json_file(path), topic_ids),
            "news-hotspots-runtime.json": lambda path: validate_runtime(load_json_file(path)),
        }
        for file_name, validator in overlay_checks.items():
            path = config_dir / file_name
            if path.exists():
                errors.extend(validator(path))

    # Verify merged loaders still work on the validated files.
    try:
        load_merged_rss_sources(args.defaults, config_dir)
        load_merged_twitter_sources(args.defaults, config_dir)
        load_merged_github_sources(args.defaults, config_dir)
        load_merged_reddit_sources(args.defaults, config_dir)
        load_merged_topics(args.defaults, config_dir)
        load_merged_api_sources(args.defaults, config_dir)
        load_merged_runtime_config(args.defaults, config_dir)
    except Exception as exc:
        errors.append(f"merged config loading failed: {exc}")

    if errors:
        for error in errors:
            logger.error(error)
        return 1

    logger.info("✅ config validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
