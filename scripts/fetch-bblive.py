#!/usr/bin/env python3
"""
BB Live（bb-browser 列表源）抓取脚本。

职责：
- 顺序调用 `bb-browser site` 抓取多个列表型 adapter：
  - 36kr/newsflash   → 36氪快讯（business）
  - eastmoney/news   → 东方财富财经新闻（business）
  - bilibili/trending → B站热搜（social）
- 标准化为统一 `articles`，与 fetch-toutiao 同一输出契约
- 抓取耗时、失败信息写入 `*.meta.json`

输出文件职责：
- `<step>.json` 只保存抓到的 article 列表
- `<step>.meta.json` 只保存 step 级诊断
"""

import argparse
import html
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

try:
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, local_now, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, local_now, write_result_with_meta

STEP_KEY = "bblive"
DEFAULT_TIMEOUT = 45
DEFAULT_LIMIT = 20
COOLDOWN_SECONDS = 5.0

# (source_id, name, adapter, items_key, topic, priority)
BBLIVE_SOURCES: List[Dict[str, Any]] = [
    {
        "source_id": "36kr-newsflash",
        "name": "36氪快讯",
        "adapter": "36kr/newsflash",
        "items_key": "items",
        "topic": "business",
        "priority": 3,
    },
    {
        "source_id": "eastmoney-news",
        "name": "东方财富财经新闻",
        "adapter": "eastmoney/news",
        "items_key": "news",
        "topic": "business",
        "priority": 3,
    },
    {
        "source_id": "bilibili-trending",
        "name": "B站热搜",
        "adapter": "bilibili/trending",
        "items_key": "items",
        "topic": "social",
        "priority": 3,
    },
]

_last_success_at: Optional[float] = None


class TimedRuntimeError(RuntimeError):
    def __init__(self, message: str, elapsed_s: float):
        super().__init__(message)
        self.elapsed_s = elapsed_s


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    return logging.getLogger(__name__)


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global DEFAULT_TIMEOUT, DEFAULT_LIMIT, COOLDOWN_SECONDS
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get(STEP_KEY, {})
    diagnostics_config = runtime.get("diagnostics", {})
    DEFAULT_TIMEOUT = int(fetch_config.get("request_timeout_s", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    DEFAULT_LIMIT = int(fetch_config.get("limit", DEFAULT_LIMIT) or DEFAULT_LIMIT)
    COOLDOWN_SECONDS = float(fetch_config.get("cooldown_s", COOLDOWN_SECONDS) or 0)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def throttle_after_success() -> None:
    global _last_success_at
    if _last_success_at is None:
        return
    wait_seconds = COOLDOWN_SECONDS - (time.monotonic() - _last_success_at)
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def run_bb_browser_site(command: Sequence[str], timeout: Optional[int] = None) -> Dict[str, Any]:
    global _last_success_at
    throttle_after_success()
    effective_timeout = int(timeout if timeout is not None else DEFAULT_TIMEOUT)
    request_started_at = time.monotonic()
    result = subprocess.run(
        ["bb-browser", "site", *command],
        capture_output=True,
        text=True,
        timeout=effective_timeout,
        env=os.environ,
    )
    elapsed_s = time.monotonic() - request_started_at
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "bb-browser command failed").strip()
        raise TimedRuntimeError(message, elapsed_s)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TimedRuntimeError(f"Invalid JSON from bb-browser: {exc}", elapsed_s) from exc
    _last_success_at = time.monotonic()
    return payload


def clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def truncate_summary(value: Any, limit: int = 240) -> str:
    text = clean_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def first_non_empty(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def parse_number(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    return None


def parse_iso_to_local(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone()
        return parsed.isoformat()
    except ValueError:
        return None


def parse_eastmoney_time(value: Any) -> Optional[str]:
    """东方财富 time 形如 '2026-10-31 02:14:00'，按本地时区解析。"""
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        return parsed.isoformat()
    except ValueError:
        return parse_iso_to_local(text)


def transform_36kr_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = first_non_empty(item.get("title"))
    if not title:
        return None
    return {
        "title": title,
        "link": first_non_empty(item.get("url")),
        "date": parse_iso_to_local(item.get("timestamp")) or local_now().isoformat(),
        "summary": truncate_summary(item.get("description")),
        "source_name": "36氪快讯",
        "topic": "business",
        "hot_score": 0,
        "rank": parse_number(item.get("rank")) or 0,
        "metrics": {"rank": parse_number(item.get("rank"))},
    }


def transform_eastmoney_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = first_non_empty(item.get("title"))
    if not title:
        return None
    return {
        "title": title,
        "link": first_non_empty(item.get("url")),
        "date": parse_eastmoney_time(item.get("time")) or local_now().isoformat(),
        "summary": truncate_summary(item.get("summary")),
        "source_name": "东方财富财经新闻",
        "topic": "business",
        "hot_score": 0,
        "rank": parse_number(item.get("rank")) or 0,
        "metrics": {"rank": parse_number(item.get("rank")), "origin": first_non_empty(item.get("source"))},
    }


def transform_bilibili_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = first_non_empty(item.get("keyword"), item.get("show_name"), item.get("name"))
    if not title:
        return None
    return {
        "title": title,
        "link": first_non_empty(item.get("search_url"), item.get("url")),
        "date": local_now().isoformat(),
        "summary": "",
        "source_name": "B站热搜",
        "topic": "social",
        "hot_score": parse_number(item.get("hot_score")) or 0,
        "rank": parse_number(item.get("rank")) or 0,
        "metrics": {"rank": parse_number(item.get("rank")), "is_hot": bool(item.get("is_hot"))},
    }


TRANSFORMERS = {
    "36kr/newsflash": transform_36kr_item,
    "eastmoney/news": transform_eastmoney_item,
    "bilibili/trending": transform_bilibili_item,
}


def fetch_bblive_source(spec: Dict[str, Any], limit: int, logger: logging.Logger) -> Dict[str, Any]:
    adapter = spec["adapter"]
    request_started_at = time.monotonic()
    try:
        payload = run_bb_browser_site([adapter, str(limit)])
    except (TimedRuntimeError, subprocess.TimeoutExpired) as exc:
        elapsed_s = time.monotonic() - request_started_at
        message = str(exc)
        logger.warning("⚠️ %s failed: %s", spec["name"], message)
        return {
            "source_id": spec["source_id"],
            "source_type": STEP_KEY,
            "name": spec["name"],
            "priority": spec["priority"],
            "topic": spec["topic"],
            "status": "error",
            "error": message[:300],
            "elapsed_s": round(elapsed_s, 3),
            "items": 0,
            "count": 0,
            "articles": [],
            "request_traces": [
                build_request_trace(
                    spec["source_id"],
                    adapter,
                    elapsed_s,
                    status="error",
                    error=message[:300],
                    source_type=STEP_KEY,
                    method="CLI",
                    attempt=1,
                    backend="bb-browser",
                    adapter=adapter,
                )
            ],
        }

    elapsed_s = time.monotonic() - request_started_at
    raw_items = payload.get(spec["items_key"]) or []
    if not isinstance(raw_items, list):
        raw_items = []
    transformer = TRANSFORMERS[adapter]
    articles: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        article = transformer(item)
        if article:
            articles.append(article)

    seen_links: set = set()
    deduped: List[Dict[str, Any]] = []
    for article in articles:
        key = article["link"] or article["title"]
        if key in seen_links:
            continue
        seen_links.add(key)
        deduped.append(article)

    logger.info("Fetched %s: %d raw, %d kept", spec["name"], len(raw_items), len(deduped))
    return {
        "source_id": spec["source_id"],
        "source_type": STEP_KEY,
        "name": spec["name"],
        "priority": spec["priority"],
        "topic": spec["topic"],
        "status": "ok" if deduped else "error",
        "elapsed_s": round(elapsed_s, 3),
        "items": len(deduped),
        "count": len(deduped),
        "fetched_count": len(raw_items),
        "articles": deduped,
        "request_traces": [
            build_request_trace(
                spec["source_id"],
                adapter,
                elapsed_s,
                status="ok",
                source_type=STEP_KEY,
                method="CLI",
                attempt=1,
                backend="bb-browser",
                adapter=adapter,
            )
        ],
    }


def fetch_bblive(logger: logging.Logger, limit: int) -> Dict[str, Any]:
    logger.info("BBLive bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
    source_results = [fetch_bblive_source(spec, limit, logger) for spec in BBLIVE_SOURCES]
    articles: List[Dict[str, Any]] = []
    traces: List[Dict[str, Any]] = []
    ok_sources = 0
    for result in source_results:
        articles.extend(result["articles"])
        traces.extend(result["request_traces"])
        if result["status"] == "ok":
            ok_sources += 1
    return {
        "generated": local_now().isoformat(),
        "source_type": STEP_KEY,
        "calls_total": len(source_results),
        "calls_ok": ok_sources,
        "items_total": len(articles),
        "sources_total": len(source_results),
        "sources_ok": ok_sources,
        "total_articles": len(articles),
        "articles": articles,
        "sources": source_results,
        "request_traces": traces,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch bb-browser list sources (36kr/eastmoney/bilibili).")
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Accepted for CLI consistency; not used by BBLive fetch")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    parser.add_argument("--limit", type=int, default=None, help="Number of items to request per source")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-bblive-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        step_started_at = time.monotonic()
        limit = args.limit if args.limit is not None else DEFAULT_LIMIT
        data = fetch_bblive(logger, max(1, int(limit)))
        elapsed_active_s = sum(float(r.get("elapsed_s", 0) or 0) for r in data["sources"])
        output = {"generated": data.get("generated"), "source_type": STEP_KEY, "articles": data.get("articles", [])}
        meta = build_step_meta(
            step_key=STEP_KEY,
            status="ok" if data["calls_ok"] == data["calls_total"] and data["items_total"] > 0 else ("partial" if data["calls_ok"] > 0 else "error"),
            elapsed_active_s=elapsed_active_s,
            elapsed_total_s=time.monotonic() - step_started_at,
            items=int(data["items_total"]),
            calls_total=int(data["calls_total"]),
            calls_ok=int(data["calls_ok"]),
            failed_items=None,
            request_traces=data.get("request_traces", []),
        )
        write_result_with_meta(args.output, output, meta)
        logger.info("✅ Done: %d/%d sources ok, %d articles → %s", data["sources_ok"], data["sources_total"], data["total_articles"], args.output)
        return 0 if data["items_total"] else 1
    except Exception as exc:  # noqa: BLE001
        logger.error("💥 BBLive fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
