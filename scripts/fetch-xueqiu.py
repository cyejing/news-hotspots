#!/usr/bin/env python3
"""
Xueqiu 用户动态抓取脚本（默认抓取段永平雪球动态）。

职责：
- 调用 `bb-browser site xueqiu/user` 获取指定雪球用户的动态
- 将动态条目标准化为统一 `articles`（topic=value-investing）
- 将抓取耗时、失败信息和慢请求统计写入 `*.meta.json`

执行逻辑：
1. 加载 runtime 配置
2. 顺序执行 Xueqiu 抓取并遵守 cooldown
3. 将结果转换为统一 article 结构
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存抓到的 Xueqiu article
- `<step>.meta.json`
  只保存 step 级诊断与失败明细
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
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, local_now, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, local_now, normalize_failed_item, write_result_with_meta

# 段永平雪球 ID（"大道无形我有型"，screen_name=大道无形我有型）
DEFAULT_USER_ID = "1247347556"
DEFAULT_USER_NAME = "段永平"

SOURCE_ID = "xueqiu-duan-yongping"
SOURCE_NAME = "段永平 雪球动态"
SOURCE_PRIORITY = 4
SOURCE_TOPIC = "value-investing"
DEFAULT_TIMEOUT = 120
DEFAULT_LIMIT = 20
COOLDOWN_SECONDS = 6.0

_last_success_at: Optional[float] = None
_last_request_elapsed_s: Optional[float] = None


class TimedRuntimeError(RuntimeError):
    def __init__(self, message: str, elapsed_s: float):
        super().__init__(message)
        self.elapsed_s = elapsed_s


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(__name__)


def throttle_after_success() -> None:
    global _last_success_at
    if _last_success_at is None:
        return
    wait_seconds = COOLDOWN_SECONDS - (time.monotonic() - _last_success_at)
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global DEFAULT_TIMEOUT, DEFAULT_LIMIT, COOLDOWN_SECONDS
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("xueqiu", {})
    diagnostics_config = runtime.get("diagnostics", {})
    DEFAULT_TIMEOUT = int(fetch_config.get("request_timeout_s", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    DEFAULT_LIMIT = int(fetch_config.get("limit", DEFAULT_LIMIT) or DEFAULT_LIMIT)
    COOLDOWN_SECONDS = float(fetch_config.get("cooldown_s", COOLDOWN_SECONDS) or 0)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def clear_last_request_elapsed() -> None:
    global _last_request_elapsed_s
    _last_request_elapsed_s = None


def last_request_elapsed(default: float = 0.0) -> float:
    return float(_last_request_elapsed_s if _last_request_elapsed_s is not None else default)


def run_bb_browser_site(command: Sequence[str], timeout: Optional[int] = None) -> Dict[str, Any]:
    global _last_success_at, _last_request_elapsed_s
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
    _last_request_elapsed_s = elapsed_s
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


def truncate_summary(value: Any, limit: int = 280) -> str:
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
    if isinstance(value, (int, float)):
        return int(value)
    text = clean_text(value).lower().replace(",", "")
    if not text:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        return None
    return int(float(match.group(1)))


def extract_user_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("error"), str):
            return []
        for key in ("items", "statuses", "list", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                for nested_key in ("items", "statuses", "list"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return [item for item in nested if isinstance(item, dict)]
    return []


def parse_created_at(value: Any) -> str:
    """雪球 created_at 可能是毫秒时间戳或 ISO 字符串，统一输出 ISO 字符串。"""
    if not value:
        return ""
    text = str(value).strip()
    if text.isdigit():
        ts = int(text)
        if ts > 1e12:  # 毫秒
            ts = ts // 1000
        try:
            return datetime.utcfromtimestamp(ts).isoformat() + "Z"
        except (ValueError, OSError, OverflowError):
            return ""
    return text


def transform_user_item(item: Dict[str, Any], user_name: str) -> Optional[Dict[str, Any]]:
    text = first_non_empty(item.get("text"), item.get("description"), item.get("title"))
    if not text:
        return None

    link = first_non_empty(item.get("url"), item.get("link"))
    item_id = first_non_empty(item.get("id"), item.get("post_id"))
    author_id = str(item.get("author_id") or item.get("user_id") or "")
    if not link and item_id and author_id:
        link = f"https://xueqiu.com/{author_id}/{item_id}"
    if not link:
        return None

    return {
        "title": truncate_summary(text, 120),
        "link": link,
        "date": parse_created_at(item.get("created_at")),
        "summary": truncate_summary(text, 280),
        "source_name": SOURCE_NAME,
        "topic": SOURCE_TOPIC,
        "author": first_non_empty(item.get("author"), item.get("screen_name")) or user_name,
        "author_id": author_id,
        "likes": parse_number(item.get("likes") or item.get("fav_count")) or 0,
        "retweets": parse_number(item.get("retweets") or item.get("retweet_count")) or 0,
        "replies": parse_number(item.get("replies") or item.get("reply_count")) or 0,
        "metrics": {
            "likes": parse_number(item.get("likes") or item.get("fav_count")),
            "retweets": parse_number(item.get("retweets") or item.get("retweet_count")),
            "replies": parse_number(item.get("replies") or item.get("reply_count")),
        },
    }


def fetch_xueqiu_user(
    logger: logging.Logger,
    defaults_dir: Optional[Path] = None,
    config_dir: Optional[Path] = None,
    user_id: str = DEFAULT_USER_ID,
    limit: int = DEFAULT_LIMIT,
) -> Dict[str, Any]:
    logger.info("Xueqiu bb-browser cooldown: %.1fs", COOLDOWN_SECONDS)
    clear_last_request_elapsed()
    payload = run_bb_browser_site(["xueqiu/user", user_id, str(limit)])
    elapsed_s = last_request_elapsed()

    if isinstance(payload, dict) and isinstance(payload.get("error"), str):
        raise TimedRuntimeError(payload.get("error") or "xueqiu/user adapter error", elapsed_s)

    request_trace = build_request_trace(
        SOURCE_ID,
        f"xueqiu/user:{user_id}",
        elapsed_s,
        status="ok",
        source_type="xueqiu",
        method="CLI",
        attempt=1,
        backend="bb-browser",
        adapter="xueqiu/user",
    )
    raw_items = extract_user_items(payload)
    articles: List[Dict[str, Any]] = []
    for item in raw_items:
        article = transform_user_item(item, DEFAULT_USER_NAME)
        if article:
            articles.append(article)

    logger.info("Fetched Xueqiu user timeline [%s]: %d raw, %d kept", user_id, len(raw_items), len(articles))

    source_result = {
        "source_id": SOURCE_ID,
        "source_type": "xueqiu",
        "name": SOURCE_NAME,
        "priority": SOURCE_PRIORITY,
        "topic": SOURCE_TOPIC,
        "user_id": user_id,
        "status": "ok" if articles else "error",
        "elapsed_s": round(elapsed_s, 3),
        "items": len(articles),
        "count": len(articles),
        "fetched_count": len(raw_items),
        "articles": articles,
        "request_traces": [request_trace],
    }
    if not articles:
        source_result["error"] = "No items from xueqiu/user (adapter returned empty or not logged in)"

    return {
        "generated": local_now().isoformat(),
        "source_type": "xueqiu",
        "calls_total": 1,
        "calls_ok": 1 if articles else 0,
        "items_total": len(articles),
        "sources_total": 1,
        "sources_ok": 1 if articles else 0,
        "total_articles": len(articles),
        "sources": [source_result],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Xueqiu user timeline (段永平 by default) via bb-browser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-xueqiu.py --defaults config/defaults --config workspace/config --output xueqiu.json
    python3 fetch-xueqiu.py --verbose
    python3 fetch-xueqiu.py --user-id 6175873088 --limit 20
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Accepted for CLI consistency; not used by Xueqiu fetch")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    parser.add_argument("--limit", type=int, default=None, help="Number of user timeline items to request")
    parser.add_argument("--user-id", type=str, default=DEFAULT_USER_ID, help="Xueqiu user ID (default: 段永平 6175873088)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-xueqiu-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        step_started_at = time.monotonic()
        limit = args.limit if args.limit is not None else DEFAULT_LIMIT
        data = fetch_xueqiu_user(
            logger,
            defaults_dir=args.defaults,
            config_dir=effective_config_dir,
            user_id=args.user_id,
            limit=max(1, int(limit)),
        )
        source_result = (data.get("sources") or [{}])[0]
        output = {"generated": data.get("generated"), "source_type": "xueqiu", "articles": source_result.get("articles", [])}
        meta = build_step_meta(
            step_key="xueqiu",
            status="ok" if int(data.get("calls_ok", 0) or 0) == int(data.get("calls_total", 1) or 1) and int(data.get("items_total", 0) or 0) > 0 else "error",
            elapsed_active_s=float(source_result.get("elapsed_s", 0) or 0),
            elapsed_total_s=time.monotonic() - step_started_at,
            items=int(data.get("items_total", 0) or 0),
            calls_total=int(data.get("calls_total", 1) or 1),
            calls_ok=int(data.get("calls_ok", 0) or 0),
            failed_items=None,
            request_traces=source_result.get("request_traces", []),
        )
        write_result_with_meta(args.output, output, meta)
        logger.info(
            "✅ Done: %d/%d sources ok, %d articles → %s",
            data.get("sources_ok", 0),
            data.get("sources_total", 0),
            data.get("total_articles", 0),
            args.output,
        )
        return 0 if data.get("items_total", 0) else 1
    except Exception as exc:
        logger.error("💥 Xueqiu fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
