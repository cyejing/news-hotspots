#!/usr/bin/env python3
"""
news-hotspots 配置加载层。

职责：
- 读取 `config/defaults/` 下的默认配置文件
- 读取 `<WORKSPACE>/config/` 下的同名 overlay 配置
- 以“defaults + overlay 深合并”的方式生成运行时配置对象

执行逻辑：
1. 先读取 source_type 对应的 defaults 文件
2. 如果 workspace overlay 存在，再按 id 做覆盖合并
3. 为 fetch / merge / pipeline 脚本提供统一配置入口

配置文件职责：
- `rss.json` / `twitter.json` / `github.json` / `reddit.json` / `api.json`
  保存各 source_type 的业务输入配置
- `topics.json`
  保存 topic 查询配置
- `runtime.json`
  保存 timeout、cooldown、并发、诊断阈值等运行参数
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

RSS_DEFAULTS_FILE = "rss.json"
TWITTER_DEFAULTS_FILE = "twitter.json"
GITHUB_DEFAULTS_FILE = "github.json"
REDDIT_DEFAULTS_FILE = "reddit.json"
TOPICS_DEFAULTS_FILE = "topics.json"
API_SOURCES_DEFAULTS_FILE = "api.json"
RUNTIME_DEFAULTS_FILE = "runtime.json"

RSS_OVERLAY_FILE = "news-hotspots-rss.json"
TWITTER_OVERLAY_FILE = "news-hotspots-twitter.json"
GITHUB_OVERLAY_FILE = "news-hotspots-github.json"
REDDIT_OVERLAY_FILE = "news-hotspots-reddit.json"
TOPICS_OVERLAY_FILE = "news-hotspots-topics.json"
API_SOURCES_OVERLAY_FILE = "news-hotspots-api.json"
RUNTIME_OVERLAY_FILE = "news-hotspots-runtime.json"


def deep_merge_dicts(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = deep_merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_json_object(path: Path, label: str) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{label} not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {label} {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return data


def _merge_record_lists(defaults: List[Dict[str, Any]], overlay: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged_by_id: Dict[str, Dict[str, Any]] = {}
    default_ids: List[str] = []
    for record in defaults:
        record_id = str(record.get("id") or "").strip()
        if not record_id:
            continue
        merged_by_id[record_id] = dict(record)
        default_ids.append(record_id)

    overlay_ids: List[str] = []
    for record in overlay:
        record_id = str(record.get("id") or "").strip()
        if not record_id:
            continue
        merged_by_id[record_id] = deep_merge_dicts(merged_by_id.get(record_id, {}), dict(record))
        overlay_ids.append(record_id)

    result: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for record_id in default_ids + overlay_ids:
        if record_id in seen:
            continue
        if record_id in merged_by_id:
            result.append(merged_by_id[record_id])
            seen.add(record_id)
    return result


def _load_merged_record_file(
    defaults_dir: Path,
    defaults_filename: str,
    config_dir: Optional[Path],
    overlay_filename: str,
    *,
    key: str = "sources",
) -> List[Dict[str, Any]]:
    defaults_path = defaults_dir / defaults_filename
    defaults_data = _load_json_object(defaults_path, defaults_filename)
    defaults_records = defaults_data.get(key, [])
    if not isinstance(defaults_records, list):
        raise ValueError(f"{defaults_filename} field '{key}' must be an array")

    if config_dir is None:
        return [dict(record) for record in defaults_records if isinstance(record, dict)]

    overlay_path = config_dir / overlay_filename
    try:
        overlay_data = _load_json_object(overlay_path, overlay_filename)
    except FileNotFoundError:
        logger.debug("No user config found at %s, using defaults only", overlay_path)
        return [dict(record) for record in defaults_records if isinstance(record, dict)]
    overlay_records = overlay_data.get(key, [])
    if not isinstance(overlay_records, list):
        raise ValueError(f"{overlay_filename} field '{key}' must be an array")
    return _merge_record_lists(
        [dict(record) for record in defaults_records if isinstance(record, dict)],
        [dict(record) for record in overlay_records if isinstance(record, dict)],
    )


def load_merged_rss_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return _load_merged_record_file(defaults_dir, RSS_DEFAULTS_FILE, config_dir, RSS_OVERLAY_FILE)


def load_merged_twitter_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return _load_merged_record_file(defaults_dir, TWITTER_DEFAULTS_FILE, config_dir, TWITTER_OVERLAY_FILE)


def load_merged_github_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return _load_merged_record_file(defaults_dir, GITHUB_DEFAULTS_FILE, config_dir, GITHUB_OVERLAY_FILE)


def load_merged_reddit_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return _load_merged_record_file(defaults_dir, REDDIT_DEFAULTS_FILE, config_dir, REDDIT_OVERLAY_FILE)


def load_merged_topics(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return _load_merged_record_file(defaults_dir, TOPICS_DEFAULTS_FILE, config_dir, TOPICS_OVERLAY_FILE, key="topics")


def load_merged_api_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    return _load_merged_record_file(defaults_dir, API_SOURCES_DEFAULTS_FILE, config_dir, API_SOURCES_OVERLAY_FILE)


def load_merged_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    defaults_path = defaults_dir / RUNTIME_DEFAULTS_FILE
    defaults_data = _load_json_object(defaults_path, RUNTIME_DEFAULTS_FILE)
    if config_dir is None:
        return defaults_data

    overlay_path = config_dir / RUNTIME_OVERLAY_FILE
    try:
        overlay_data = _load_json_object(overlay_path, RUNTIME_OVERLAY_FILE)
    except FileNotFoundError:
        logger.debug("No user runtime config found at %s, using defaults only", overlay_path)
        return defaults_data
    return deep_merge_dicts(defaults_data, overlay_data)
