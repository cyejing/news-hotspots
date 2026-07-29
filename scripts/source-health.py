#!/usr/bin/env python3
"""
热点抓取健康诊断脚本。

职责：
- 发现 archive 下的 `meta/*.meta.json`
- 归一化不同 step 的诊断字段
- 分析日志模式、慢请求占比、items 趋势与连续失败
- 输出当前运行与最近历史的健康报告

执行逻辑：
1. 接收某天的 `meta/` 目录或 archive 根目录
2. 自动收集最近若干天的 `*.meta.json`
3. 聚合失败项、耗时、慢请求、日志错误模式与趋势
4. 以文本方式输出 History report 与 Run details

输入文件职责：
- `meta/*.meta.json`
  诊断源数据，是 health 报告唯一输入

说明：
- 本脚本不回读 fetch 结果 JSON，也不读取 `merge-sources.json` / `daily.json`
- 日志模式分析读取 meta 中已有的 `logs.tail`（stdout/stderr 末尾行）与 `failed_items[].error`
- 不引入新的运行日志文件，全部基于 step meta 已承载的结构化字段
"""

import argparse
import logging
import re
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

try:
    from config_loader import load_merged_runtime_config
    from step_contract import from_timestamp_local, local_now, local_tzinfo
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_contract import from_timestamp_local, local_now, local_tzinfo

HISTORY_DAYS = 7
DEGRADED_THRESHOLD = 0.5
REPORT_LIMIT = 20
ERROR_TEXT_LIMIT = 180
META_FILE_RE = re.compile(r".*(?:\.meta\d*|\d+\.meta|\.meta)\.json$")
META_SUFFIX_RE = re.compile(r"(?:(?:\.meta(\d*))|(?:(\d+)\.meta))\.json$")

# 日志错误模式默认关键词表（可被 runtime.diagnostics.log_pattern_keywords 覆盖）
DEFAULT_LOG_PATTERN_KEYWORDS: Dict[str, List[str]] = {
    "timeout": ["timed out", "timeout", "超时"],
    "rate_limited": ["429", "rate limit", "too many requests", "频次"],
    "connection": ["connection", "refused", "reset", "unreachable", "dns", "连接"],
    "parse": ["json", "decode", "parse", "解析"],
    "empty": ["empty", "no results", "无结果", "no tweets", "no items"],
}
LOG_PATTERN_ORDER = ["timeout", "rate_limited", "connection", "parse", "empty"]
LOG_PATTERN_ICONS = {
    "timeout": "⏱️",
    "rate_limited": "🚦",
    "connection": "🔌",
    "parse": "🐛",
    "empty": "📭",
}
CONSECUTIVE_FAIL_DAYS_THRESHOLD = 2
ITEMS_DECLINE_RATIO = 0.5  # 相比 7 天中位数下降超过一半视为趋势告警


def resolve_config_dir(config_dir: Optional[Path]) -> Optional[Path]:
    if config_dir is None:
        return None
    return config_dir if config_dir.exists() else None


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path]) -> Dict[str, Any]:
    """从 runtime 配置加载诊断阈值；保留全局变量以兼容现有测试契约。"""
    global HISTORY_DAYS, DEGRADED_THRESHOLD, REPORT_LIMIT, ERROR_TEXT_LIMIT
    runtime = load_merged_runtime_config(defaults_dir, resolve_config_dir(config_dir))
    diagnostics = runtime.get("diagnostics", {}) if isinstance(runtime.get("diagnostics"), dict) else {}
    HISTORY_DAYS = int(diagnostics.get("history_days", HISTORY_DAYS) or HISTORY_DAYS)
    DEGRADED_THRESHOLD = float(diagnostics.get("degraded_threshold", DEGRADED_THRESHOLD) or DEGRADED_THRESHOLD)
    REPORT_LIMIT = int(diagnostics.get("report_limit", REPORT_LIMIT) or REPORT_LIMIT)
    ERROR_TEXT_LIMIT = int(diagnostics.get("error_text_limit", ERROR_TEXT_LIMIT) or ERROR_TEXT_LIMIT)
    return runtime


@dataclass
class DiagnosticRecord:
    step_key: str
    name: str
    status: str
    state: str
    elapsed_s: float
    items: int
    call_stats: Dict[str, Any]
    failed_items: List[Dict[str, Any]]
    details: Dict[str, Any]
    observed_ts: float
    run_label: Optional[str] = None
    log_patterns: Dict[str, int] = field(default_factory=dict)
    slow_request_rate: float = 0.0


@dataclass
class HistoryRow:
    step_key: str
    name: str
    checks: int
    ok: int
    warn: int
    error: int
    degraded_rate: float
    unhealthy: bool
    median_elapsed_s: float
    latest_issue_ts: Optional[float]
    latest_issue_summary: str
    check_details: List[Dict[str, Any]]
    median_items: float = 0.0
    consecutive_fail_days: int = 0
    dominant_log_pattern: str = ""
    slow_request_rate: float = 0.0


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(message)s")
    return logging.getLogger(__name__)


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def trim_error_text(value: Any, limit: int = ERROR_TEXT_LIMIT) -> str:
    if value is None:
        return ""
    lines = [str(line).strip() for line in str(value).splitlines() if str(line).strip()]
    if not lines:
        return ""
    text = " | ".join(lines[:2])
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 1)].rstrip() + "..."


def discover_meta_files(input_dir: Path) -> List[Path]:
    if not input_dir.exists():
        return []
    paths = sorted(
        path
        for path in input_dir.glob("*.json")
        if META_FILE_RE.match(path.name)
    )
    pipeline_paths = [path for path in paths if path.name.startswith("pipeline.meta")]
    for pipeline_path in reversed(pipeline_paths):
        paths.remove(pipeline_path)
        paths.insert(0, pipeline_path)
    return paths


def discover_archive_meta_files(archive_dir: Path, days: int = HISTORY_DAYS) -> List[Path]:
    if not archive_dir.exists():
        return []
    cutoff = (local_now() - timedelta(days=days)).date()
    paths: List[Path] = []
    for date_dir in sorted(archive_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        try:
            dir_date = datetime.strptime(date_dir.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if dir_date < cutoff:
            continue
        meta_dir = date_dir / "meta"
        if not meta_dir.exists():
            continue
        paths.extend(discover_meta_files(meta_dir))
    return paths


def discover_all_meta_files(input_dir: Path, days: int = HISTORY_DAYS) -> List[Path]:
    direct_files = discover_meta_files(input_dir)
    archive_files = discover_archive_meta_files(input_dir, days)
    combined: List[Path] = []
    seen: set[Path] = set()
    for path in direct_files + archive_files:
        if path in seen:
            continue
        seen.add(path)
        combined.append(path)
    return combined


def parse_archive_observed_ts(path: Path) -> float:
    for parent in path.parents:
        try:
            date_value = datetime.strptime(parent.name, "%Y-%m-%d").replace(tzinfo=local_tzinfo())
            return date_value.timestamp()
        except ValueError:
            continue
    return time.time()


def parse_archive_run_label(path: Path) -> Optional[str]:
    date_label: Optional[str] = None
    for parent in path.parents:
        try:
            datetime.strptime(parent.name, "%Y-%m-%d")
            date_label = parent.name
            break
        except ValueError:
            continue
    if not date_label:
        return None

    match = META_SUFFIX_RE.search(path.name)
    if not match:
        return None
    suffix = match.group(1) if match.group(1) is not None else match.group(2)
    run_index = 1 if suffix == "" else int(suffix) + 1
    return f"{date_label}-{run_index}"


def build_direct_run_label(input_dir: Path, now_ts: float) -> str:
    date_label = from_timestamp_local(now_ts).strftime("%Y-%m-%d")
    return f"{date_label}-current"


def build_failed_items(items: Any) -> List[Dict[str, Any]]:
    return [
        dict(
            {
                "source_id": str(item.get("source_id", "item")).strip() or "item",
                "target": str(item.get("target", "")).strip(),
                "status": str(item.get("status", "error")).strip() or "error",
                "error": trim_error_text(item.get("error")),
                "method": str(item.get("method", "")).strip(),
                "attempt": int(item.get("attempt", 1) or 1),
                "backend": str(item.get("backend", "")).strip(),
                "adapter": str(item.get("adapter", "")).strip(),
            },
            **({"timing_s": item.get("timing_s")} if isinstance(item.get("timing_s"), dict) else {}),
        )
        for item in (items or [])
        if isinstance(item, dict) and trim_error_text(item.get("error"))
    ]


def format_elapsed_suffix(elapsed_s: Any) -> str:
    try:
        elapsed = float(elapsed_s or 0)
    except (TypeError, ValueError):
        return ""
    return f" | elapsed:{elapsed:.1f}s"


def normalize_call_stats(call_stats: Any, *, kind: str, total_calls: int, ok_calls: int, failed_calls: int, partial_calls: int = 0) -> Dict[str, Any]:
    payload = {
        "kind": str((call_stats or {}).get("kind", kind) if isinstance(call_stats, dict) else kind),
        "total_calls": int((call_stats or {}).get("total_calls", total_calls) if isinstance(call_stats, dict) else total_calls or 0),
        "ok_calls": int((call_stats or {}).get("ok_calls", ok_calls) if isinstance(call_stats, dict) else ok_calls or 0),
        "failed_calls": int((call_stats or {}).get("failed_calls", failed_calls) if isinstance(call_stats, dict) else failed_calls or 0),
    }
    normalized_partial = int((call_stats or {}).get("partial_calls", partial_calls) if isinstance(call_stats, dict) else partial_calls or 0)
    if normalized_partial > 0:
        payload["partial_calls"] = normalized_partial
    return payload


# ---------------------------------------------------------------------------
# 日志模式分析
# ---------------------------------------------------------------------------

def collect_log_texts(meta: Dict[str, Any]) -> List[str]:
    """从 meta 收集所有可用于错误模式匹配的文本片段。"""
    texts: List[str] = []
    logs = meta.get("logs") if isinstance(meta.get("logs"), dict) else None
    if logs:
        tail = logs.get("tail") if isinstance(logs.get("tail"), list) else []
        for line in tail:
            stripped = str(line or "").strip()
            # 去掉 stdout/stderr 前缀后保留有效内容
            if stripped.startswith("[stdout] ") or stripped.startswith("[stderr] "):
                stripped = stripped.split("] ", 1)[1] if "] " in stripped else stripped
            if stripped:
                texts.append(stripped)
    for item in (meta.get("failed_items") or []):
        if isinstance(item, dict):
            error = item.get("error")
            if error:
                texts.append(str(error))
    return texts


def classify_log_patterns(texts: Sequence[str], keywords: Dict[str, List[str]]) -> Dict[str, int]:
    """按关键词表把日志文本分类到错误模式，返回每个模式命中次数。"""
    counts: Dict[str, int] = Counter()
    for text in texts:
        lowered = text.lower()
        for pattern, words in keywords.items():
            if any(word.lower() in lowered for word in words):
                counts[pattern] += 1
                break  # 一条文本只归入最先命中的模式
    return dict(counts)


def compute_slow_request_rate(meta: Dict[str, Any]) -> float:
    slow = meta.get("slow_requests") if isinstance(meta.get("slow_requests"), dict) else None
    if not slow:
        return 0.0
    total_count = int(slow.get("total_count", 0) or 0)
    requests_total = 0
    request_timing = meta.get("request_timing_summary") if isinstance(meta.get("request_timing_summary"), dict) else None
    if request_timing:
        requests_total = int(request_timing.get("requests_total", 0) or 0)
    if requests_total <= 0:
        # 退回 call_stats.total_calls
        call_stats = meta.get("call_stats") if isinstance(meta.get("call_stats"), dict) else {}
        requests_total = int(call_stats.get("total_calls", 0) or 0)
    if requests_total <= 0:
        return 0.0
    return round(total_count / requests_total, 3)


def resolve_log_pattern_keywords(runtime: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    if not runtime:
        return dict(DEFAULT_LOG_PATTERN_KEYWORDS)
    diagnostics = runtime.get("diagnostics") if isinstance(runtime.get("diagnostics"), dict) else {}
    configured = diagnostics.get("log_pattern_keywords") if isinstance(diagnostics.get("log_pattern_keywords"), dict) else None
    if not configured:
        return dict(DEFAULT_LOG_PATTERN_KEYWORDS)
    merged = dict(DEFAULT_LOG_PATTERN_KEYWORDS)
    for pattern, words in configured.items():
        if isinstance(words, list):
            merged[str(pattern)] = [str(word) for word in words]
    return merged


# ---------------------------------------------------------------------------
# State computation
# ---------------------------------------------------------------------------

def compute_pipeline_state(meta: Dict[str, Any], observed_ts: Optional[float] = None, runtime: Optional[Dict[str, Any]] = None) -> DiagnosticRecord:
    observed_ts = observed_ts or time.time()
    keywords = resolve_log_pattern_keywords(runtime)
    if "step_summaries" in meta and isinstance(meta.get("step_summaries"), dict):
        step_summaries = meta.get("step_summaries", {})
        steps = [
            {
                "step_key": step_key,
                "name": summary.get("name", step_key),
                "status": summary.get("status", "error"),
            }
            for step_key, summary in step_summaries.items()
            if isinstance(summary, dict)
        ]
        overall_status = meta.get("status", "error")
        hotspots_status = step_summaries.get("merge-hotspots", {}).get("status", "error") if isinstance(step_summaries.get("merge-hotspots"), dict) else "error"
        merge_status = step_summaries.get("merge-sources", {}).get("status", "error") if isinstance(step_summaries.get("merge-sources"), dict) else "error"
        items = sum(
            int(summary.get("items", 0) or 0)
            for step_key, summary in step_summaries.items()
            if isinstance(summary, dict) and step_key in {"merge-hotspots", "merge-sources"}
        )
        call_stats = meta.get("call_stats", {}) if isinstance(meta.get("call_stats"), dict) else {}
        failed_items = build_failed_items([
            item
            for summary in step_summaries.values()
            if isinstance(summary, dict)
            for item in (summary.get("failed_items", []) if isinstance(summary.get("failed_items"), list) else [])
        ])
        elapsed_s = float(meta.get("timing_s", {}).get("total", 0) or 0)
        fetch_elapsed_s = float(meta.get("fetch_timing_s", {}).get("total", 0) or 0)
    else:
        steps = [step for step in meta.get("steps", []) if isinstance(step, dict)]
        failed_items = build_failed_items(meta.get("failed_items", []))
        call_stats = meta.get("call_stats", {}) if isinstance(meta.get("call_stats"), dict) else {}
        items = int(meta.get("items", 0) or 0)
        merge_status = meta.get("merge", {}).get("status", "error") if isinstance(meta.get("merge"), dict) else "error"
        hotspots_status = meta.get("hotspots_status", "error")
        overall_status = meta.get("overall_status", "error")
        elapsed_s = float(meta.get("timing_s", {}).get("total", 0) or 0)
        fetch_elapsed_s = float(meta.get("fetch_timing_s", {}).get("total", 0) or 0)
    failed_steps = [step.get("name", "unknown") for step in steps if step.get("status") in {"error", "timeout"}]
    partial_steps = [step.get("name", "unknown") for step in steps if step.get("status") == "partial"]
    skipped_steps = [step.get("name", "unknown") for step in steps if step.get("status") == "skipped"]

    if overall_status in {"error", "timeout"} or merge_status != "ok" or hotspots_status != "ok":
        state = "error"
    elif failed_steps:
        state = "error"
    elif partial_steps:
        state = "warn"
    elif skipped_steps:
        state = "warn"
    else:
        state = "ok"
    log_patterns = classify_log_patterns(collect_log_texts(meta), keywords)
    return DiagnosticRecord(
        step_key="pipeline",
        name="Pipeline",
        status=overall_status,
        state=state,
        elapsed_s=elapsed_s,
        items=items,
        call_stats=normalize_call_stats(
            call_stats,
            kind="steps",
            total_calls=len(steps),
            ok_calls=sum(1 for step in steps if step.get("status") == "ok"),
            failed_calls=sum(1 for step in steps if step.get("status") in {"partial", "error", "timeout"}),
            partial_calls=sum(1 for step in steps if step.get("status") == "partial"),
        ),
        failed_items=failed_items,
        details={
            "pipeline": {
                "fetch_elapsed_s": fetch_elapsed_s,
                "failed_steps": failed_steps,
                "partial_steps": partial_steps,
                "skipped_steps": skipped_steps,
                "hotspots_status": hotspots_status,
                "step_count": len(steps),
            }
        },
        observed_ts=observed_ts,
        log_patterns=log_patterns,
        slow_request_rate=compute_slow_request_rate(meta),
    )


def compute_step_state(meta: Dict[str, Any], observed_ts: Optional[float] = None, runtime: Optional[Dict[str, Any]] = None) -> DiagnosticRecord:
    observed_ts = observed_ts or time.time()
    keywords = resolve_log_pattern_keywords(runtime)
    if "pipeline_version" in meta:
        return compute_pipeline_state(meta, observed_ts, runtime)

    status = meta.get("status", "error")
    details = meta.get("details", {}) if isinstance(meta.get("details"), dict) else {}
    call_stats = meta.get("call_stats", {}) if isinstance(meta.get("call_stats"), dict) else {}
    items = int(meta.get("items", 0) or 0)
    total_calls = int(call_stats.get("total_calls", 0) or 0)
    ok_calls = int(call_stats.get("ok_calls", 0) or 0)
    failed_calls = int(call_stats.get("failed_calls", 0) or 0)
    partial_calls = int(call_stats.get("partial_calls", 0) or 0)
    warning_reasons: List[str] = []

    if status in {"error", "timeout", "pending"}:
        state = "error"
    elif status == "skipped":
        state = "skipped"
    else:
        state = "ok"

    if failed_calls > 0 and state == "ok":
        state = "warn"
        warning_reasons.append(f"{failed_calls} call failures")
    if partial_calls > 0 and state == "ok":
        state = "warn"
        warning_reasons.append(f"{partial_calls} partial calls")

    deduplication = details.get("deduplication", {}) if isinstance(details.get("deduplication"), dict) else {}
    if deduplication:
        dropped = deduplication.get("dropped", 0)
        drop_ratio = deduplication.get("drop_ratio", 0.0)
        warning_reasons.append(f"dedup {dropped} dropped ({drop_ratio:.0%})")

    processing = details.get("processing", {}) if isinstance(details.get("processing"), dict) else {}
    if processing.get("scoring_version"):
        warning_reasons.append(f"scoring v{processing['scoring_version']}")

    log_patterns = classify_log_patterns(collect_log_texts(meta), keywords)
    slow_rate = compute_slow_request_rate(meta)
    if slow_rate > 0 and state == "ok":
        # 慢请求占比高不直接降级，但记入 reasons 供观察
        warning_reasons.append(f"slow {slow_rate:.0%}")
    return DiagnosticRecord(
        step_key=meta.get("step_key", "unknown"),
        name=meta.get("name", meta.get("step_key", "unknown")),
        status=status,
        state=state,
        elapsed_s=float(meta.get("timing_s", {}).get("total", 0) or 0),
        items=items,
        call_stats=normalize_call_stats(
            call_stats,
            kind=meta.get("step_key", "step"),
            total_calls=total_calls,
            ok_calls=ok_calls,
            failed_calls=failed_calls,
            partial_calls=partial_calls,
        ),
        failed_items=build_failed_items(meta.get("failed_items", [])),
        details={**details, "reasons": warning_reasons},
        observed_ts=observed_ts,
        log_patterns=log_patterns,
        slow_request_rate=slow_rate,
    )


# ---------------------------------------------------------------------------
# History aggregation
# ---------------------------------------------------------------------------

def _day_bucket(observed_ts: float) -> str:
    return from_timestamp_local(observed_ts).strftime("%Y-%m-%d")


def compute_consecutive_fail_days(checks: List[Dict[str, Any]]) -> int:
    """按天聚合，统计最近连续多少天有 error/warn。"""
    if not checks:
        return 0
    by_day: Dict[str, str] = {}
    for check in checks:
        day = _day_bucket(check["ts"])
        prev = by_day.get(day)
        if prev == "error" or check["state"] == "error":
            by_day[day] = "error"
        elif check["state"] in {"warn"} and prev != "error":
            by_day[day] = "warn"
        elif prev is None:
            by_day[day] = check["state"]
    days_sorted = sorted(by_day.keys(), reverse=True)
    consecutive = 0
    today = _day_bucket(time.time())
    cursor = today
    for day in days_sorted:
        if day != cursor:
            break
        if by_day[day] in {"error", "warn"}:
            consecutive += 1
            # 往前一天
            try:
                cursor = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            except ValueError:
                break
        else:
            break
    return consecutive


def build_history_rows(diagnostics: List[DiagnosticRecord], now: float) -> List[HistoryRow]:
    cutoff = now - HISTORY_DAYS * 86400
    grouped: Dict[str, Dict[str, Any]] = {}
    for diagnostic in diagnostics:
        observed_ts = float(diagnostic.observed_ts or now)
        if observed_ts <= cutoff:
            continue
        step_key = diagnostic.step_key
        if step_key not in grouped:
            grouped[step_key] = {"name": diagnostic.name, "checks": []}
        grouped[step_key]["name"] = diagnostic.name
        grouped[step_key]["checks"].append(
            {
                "ts": observed_ts,
                "state": diagnostic.state,
                "status": diagnostic.status,
                "elapsed_s": diagnostic.elapsed_s,
                "items": diagnostic.items,
                "call_stats": diagnostic.call_stats,
                "failed_records": diagnostic.call_stats.get("failed_calls", 0),
                "error_summary": str(diagnostic.failed_items[0]["error"]).strip() if diagnostic.failed_items else "",
                "log_patterns": dict(diagnostic.log_patterns),
                "slow_request_rate": float(diagnostic.slow_request_rate or 0.0),
                "failed_items": [
                    dict(
                        {
                            "source_id": str(item.get("source_id", "item")).strip() or "item",
                            "target": str(item.get("target", "")).strip(),
                            "status": str(item.get("status", "error")).strip() or "error",
                            "error": trim_error_text(item.get("error")),
                            "method": str(item.get("method", "")).strip(),
                            "attempt": int(item.get("attempt", 1) or 1),
                            "backend": str(item.get("backend", "")).strip(),
                            "adapter": str(item.get("adapter", "")).strip(),
                        },
                        **({"elapsed_s": float(item.get("elapsed_s", 0) or 0)} if item.get("elapsed_s") is not None else {}),
                    )
                    for item in diagnostic.failed_items
                    if isinstance(item, dict) and trim_error_text(item.get("error"))
                ][:10],
            }
        )

    rows: List[HistoryRow] = []
    for step_key, info in grouped.items():
        checks = info.get("checks", [])
        if not isinstance(checks, list) or not checks:
            continue
        ok_count = sum(1 for check in checks if check["state"] == "ok")
        warn_count = sum(1 for check in checks if check["state"] == "warn")
        error_count = sum(1 for check in checks if check["state"] == "error")
        degraded_rate = (warn_count + error_count) / len(checks)
        degraded_checks = [check for check in checks if check["state"] in {"warn", "error"}]
        latest_issue = max(degraded_checks, key=lambda check: check["ts"], default=None)
        checks_sorted = sorted(checks, key=lambda check: check["ts"], reverse=True)
        median_items = statistics.median([check["items"] for check in checks]) if checks else 0.0
        pattern_counter: Counter = Counter()
        slow_rates: List[float] = []
        for check in checks:
            for pattern, count in (check.get("log_patterns") or {}).items():
                pattern_counter[pattern] += count
            slow_rates.append(float(check.get("slow_request_rate") or 0.0))
        dominant_pattern = pattern_counter.most_common(1)[0][0] if pattern_counter else ""
        median_slow = statistics.median(slow_rates) if slow_rates else 0.0
        consecutive_fail_days = compute_consecutive_fail_days(checks)
        rows.append(
            HistoryRow(
                step_key=step_key,
                name=info.get("name", step_key),
                checks=len(checks),
                ok=ok_count,
                warn=warn_count,
                error=error_count,
                degraded_rate=degraded_rate,
                unhealthy=len(checks) >= 2 and degraded_rate > DEGRADED_THRESHOLD,
                median_elapsed_s=statistics.median([check["elapsed_s"] for check in checks]) if checks else 0.0,
                latest_issue_ts=latest_issue["ts"] if latest_issue else None,
                latest_issue_summary=latest_issue.get("error_summary", "") if latest_issue else "",
                check_details=checks_sorted,
                median_items=round(float(median_items), 1),
                consecutive_fail_days=consecutive_fail_days,
                dominant_log_pattern=dominant_pattern,
                slow_request_rate=round(float(median_slow), 3),
            )
        )
    rows.sort(key=lambda row: (-row.degraded_rate, -row.error, row.name))
    return rows


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _pattern_icon(pattern: str) -> str:
    return LOG_PATTERN_ICONS.get(pattern, "⚠️")


def render_history_report(history_rows: List[HistoryRow]) -> List[str]:
    unhealthy = [row for row in history_rows if row.unhealthy]
    lines = [f"History report: {len(history_rows)} steps tracked, {len(unhealthy)} unhealthy in last {HISTORY_DAYS} days"]
    name_width = max((len(row.name) for row in history_rows[:REPORT_LIMIT]), default=0)
    for row in history_rows[:REPORT_LIMIT]:
        if row.unhealthy or row.warn > 0 or row.error > 0:
            icon = "⚠️"
        else:
            icon = "✅"
        pattern_tag = ""
        if row.dominant_log_pattern:
            pattern_tag = f" | {_pattern_icon(row.dominant_log_pattern)} {row.dominant_log_pattern}"
        consecutive_tag = ""
        if row.consecutive_fail_days >= CONSECUTIVE_FAIL_DAYS_THRESHOLD:
            consecutive_tag = f" | 🔁 {row.consecutive_fail_days}d degrade"
        items_tag = f" | items~{row.median_items:.0f}"
        slow_tag = f" | slow {row.slow_request_rate:.0%}" if row.slow_request_rate > 0 else ""
        lines.append(
            f"{icon} {row.name:<{name_width}} - ok:{row.ok} warn:{row.warn} error:{row.error} ({row.degraded_rate:.0%} degraded)"
            f"{items_tag}{slow_tag}{pattern_tag}{consecutive_tag}"
        )
    return lines


def render_run_details(diagnostics: List[DiagnosticRecord]) -> List[str]:
    run_groups: Dict[str, List[DiagnosticRecord]] = {}
    for diagnostic in diagnostics:
        run_label = diagnostic.run_label
        if not run_label:
            continue
        run_groups.setdefault(run_label, []).append(diagnostic)

    if not run_groups:
        return []

    def sort_key(label: str) -> tuple[str, int]:
        date_part, _, run_part = label.rpartition("-")
        try:
            return (date_part, int(run_part))
        except ValueError:
            return (label, 0)

    lines = ["Run details:"]
    name_width = max((len(item.name) for items in run_groups.values() for item in items), default=0)
    for run_label in sorted(run_groups.keys(), key=sort_key, reverse=True):
        title = f"=== {run_label} ==="
        border = "=" * len(title)
        lines.extend(["", border, title, border])
        run_items = sorted(
            run_groups[run_label],
            key=lambda item: (0 if item.step_key == "pipeline" else 1, item.name),
        )
        for item in run_items:
            if item.state in {"error", "warn"}:
                icon = "⚠️"
            elif item.state == "skipped":
                icon = "⏭️"
            else:
                icon = "✅"

            call_stats = item.call_stats
            ok_calls = int(call_stats.get("ok_calls", 0) or 0)
            failed_calls = int(call_stats.get("failed_calls", 0) or 0)
            partial_calls = int(call_stats.get("partial_calls", 0) or 0)
            total_calls = int(call_stats.get("total_calls", 0) or 0)
            call_summary = f"calls:{ok_calls}/{failed_calls}/{total_calls}"
            if partial_calls > 0:
                call_summary += f" | partial:{partial_calls}"
            slow_tag = f" | slow:{item.slow_request_rate:.0%}" if item.slow_request_rate > 0 else ""
            lines.append(f"{icon} {item.name:<{name_width}} - {call_summary} | items:{item.items} | elapsed:{float(item.elapsed_s or 0):.1f}s{slow_tag}")
            for failed_item in item.failed_items:
                failed_elapsed = failed_item.get("elapsed_s", item.elapsed_s)
                lines.append(
                    f"   - {failed_item.get('source_id', 'item')}: {trim_error_text(failed_item.get('error'))}"
                    f"{format_elapsed_suffix(failed_elapsed)}"
                )
            if item.log_patterns:
                pattern_summary = ", ".join(
                    f"{_pattern_icon(p)}{p}:{n}" for p, n in sorted(item.log_patterns.items(), key=lambda kv: -kv[1])
                )
                lines.append(f"   log patterns: {pattern_summary}")
    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read step metadata and report pipeline health diagnostics.")
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Defaults config directory")
    parser.add_argument("--config", type=Path, default=None, help="Workspace overlay config directory")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Directory to inspect. Reads direct *.meta*.json files in this directory and recent <DATE>/meta/*.meta*.json below it.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime = apply_runtime_config(args.defaults, args.config)
    logger = setup_logging(args.verbose)
    now = time.time()
    current_diagnostics: List[DiagnosticRecord] = []
    meta_files = discover_all_meta_files(args.input, HISTORY_DAYS)
    direct_run_label = build_direct_run_label(args.input, now)
    for path in meta_files:
        payload = load_json(path)
        if not payload:
            logger.debug(f"skip invalid meta file: {path}")
            continue
        observed_ts = parse_archive_observed_ts(path) if "meta" in {parent.name for parent in path.parents} else now
        diagnostic = compute_step_state(payload, observed_ts, runtime)
        if "meta" in {parent.name for parent in path.parents}:
            diagnostic.run_label = parse_archive_run_label(path)
        else:
            diagnostic.run_label = direct_run_label
        current_diagnostics.append(diagnostic)

    logger.info(f"Loaded {len(current_diagnostics)} metadata files")
    history_rows = build_history_rows(current_diagnostics, now)
    for line in render_history_report(history_rows):
        logger.info(line)
    for line in render_run_details([item for item in current_diagnostics if item.run_label]):
        logger.info(line)
    unhealthy = len([row for row in history_rows if row.unhealthy])
    return 0 if unhealthy >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
