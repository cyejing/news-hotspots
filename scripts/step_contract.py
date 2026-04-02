#!/usr/bin/env python3
"""
step 结果与诊断契约公共工具。

职责：
- 统一 `<step>.json` 与 `<step>.meta.json` 的写盘方式
- 统一失败项格式、step 状态推导逻辑
- 统一请求耗时、慢请求统计和慢请求明细的结构

输出文件职责：
- `<step>.json`
  结果文件，只保存标准化结果数据
- `<step>.meta.json`
  诊断文件，只保存 step 级执行摘要、失败明细和慢请求统计
"""

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SLOW_REQUEST_THRESHOLDS = (3.0, 5.0, 10.0)


def local_now() -> datetime:
    return datetime.now().astimezone()


def local_tzinfo():
    return local_now().tzinfo


def now_iso() -> str:
    return local_now().isoformat()


def to_local_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=local_tzinfo())
    return value.astimezone(local_tzinfo())


def from_timestamp_local(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=local_tzinfo())


def local_today_iso() -> str:
    return local_now().date().isoformat()


def configure_slow_request_thresholds(thresholds: Iterable[float]) -> None:
    global SLOW_REQUEST_THRESHOLDS
    normalized: List[float] = []
    for threshold in thresholds:
        try:
            value = float(threshold)
        except (TypeError, ValueError):
            continue
        if value > 0:
            normalized.append(value)
    SLOW_REQUEST_THRESHOLDS = tuple(sorted(dict.fromkeys(normalized)))


def normalize_failed_item(
    item_id: Any,
    error: Any,
    elapsed_s: Any,
    **extra: Any,
) -> Dict[str, Any]:
    return build_request_trace(
        item_id,
        extra.pop("target", item_id),
        elapsed_s,
        status=str(extra.pop("status", "error") or "error"),
        error=error,
        **extra,
    )


def meta_path_for_output(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}.meta.json")


def build_request_trace(request_id: Any, target: Any = None, elapsed_s: Any = None, status: str = "ok", error: Any = None, **extra: Any) -> Dict[str, Any]:
    if elapsed_s is None:
        elapsed_s = target
        target = request_id
    timing = normalize_timing(
        elapsed_active_s=extra.pop("elapsed_active_s", elapsed_s),
        elapsed_total_s=extra.pop("elapsed_total_s", elapsed_s),
    )
    trace: Dict[str, Any] = {
        "source_id": str(extra.pop("source_id", request_id or target or "request")).strip() or "request",
        "target": str(target or request_id or "").strip(),
        "status": str(status or "ok"),
        "timing_s": timing,
        "attempt": max(1, int(extra.pop("attempt", 1) or 1)),
        "method": str(extra.pop("method", "") or "").strip(),
        "backend": str(extra.pop("backend", "") or "").strip(),
        "adapter": str(extra.pop("adapter", "") or "").strip(),
    }
    source_type = str(extra.pop("source_type", "") or "").strip()
    if source_type:
        trace["source_type"] = source_type
    if error not in (None, ""):
        trace["error"] = " ".join(str(error).split())[:300]
    trace.update(extra)
    return trace


def _threshold_key(threshold: float) -> str:
    return f"{float(threshold):.1f}"


def normalize_timing(elapsed_active_s: Any = None, elapsed_total_s: Any = None) -> Dict[str, float]:
    def _coerce(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return round(float(value or 0), 3)
        except (TypeError, ValueError):
            return None

    active = _coerce(elapsed_active_s)
    total = _coerce(elapsed_total_s)
    if active is None and total is None:
        active = 0.0
        total = 0.0
    elif active is None:
        active = total
    elif total is None:
        total = active
    return {"active": round(float(active or 0), 3), "total": round(float(total or 0), 3)}


def timing_active(record: Dict[str, Any]) -> float:
    timing = record.get("timing_s")
    if isinstance(timing, dict):
        try:
            return round(float(timing.get("active", 0) or 0), 3)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def timing_total(record: Dict[str, Any]) -> float:
    timing = record.get("timing_s")
    if isinstance(timing, dict):
        try:
            return round(float(timing.get("total", timing.get("active", 0)) or 0), 3)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _quantile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return round(sorted_values[0], 3)
    position = (len(sorted_values) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(sorted_values[lower], 3)
    weight = position - lower
    value = sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * weight
    return round(value, 3)


def _normalize_request_record(trace: Dict[str, Any], default_source_type: str = "") -> Dict[str, Any]:
    timing = normalize_timing(
        elapsed_active_s=(trace.get("timing_s", {}) if isinstance(trace.get("timing_s"), dict) else {}).get("active"),
        elapsed_total_s=(trace.get("timing_s", {}) if isinstance(trace.get("timing_s"), dict) else {}).get("total"),
    )
    payload = build_request_trace(
        trace.get("source_id") or trace.get("id") or trace.get("target") or "request",
        trace.get("target"),
        timing["active"],
        status=str(trace.get("status") or "ok"),
        error=trace.get("error"),
        elapsed_total_s=timing["total"],
        source_type=str(trace.get("source_type") or default_source_type or "").strip(),
        method=trace.get("method"),
        attempt=trace.get("attempt", 1),
        backend=trace.get("backend"),
        adapter=trace.get("adapter"),
    )
    for key, value in trace.items():
        if key in payload or key == "id":
            continue
        payload[key] = value
    return payload


def _build_threshold_buckets(
    traces: Iterable[Dict[str, Any]],
    default_source_type: str = "",
) -> List[Dict[str, Any]]:
    normalized = [
        _normalize_request_record(trace, default_source_type=default_source_type)
        for trace in traces
        if isinstance(trace, dict)
    ]
    total = len(normalized)
    buckets: List[Dict[str, Any]] = []
    for index, threshold in enumerate(SLOW_REQUEST_THRESHOLDS):
        upper_bound: Optional[float] = None
        if index + 1 < len(SLOW_REQUEST_THRESHOLDS):
            upper_bound = float(SLOW_REQUEST_THRESHOLDS[index + 1])
        items = []
        for trace in normalized:
            elapsed_s = timing_active(trace)
            if elapsed_s < threshold:
                continue
            if upper_bound is not None and elapsed_s >= upper_bound:
                continue
            items.append(trace)
        items.sort(key=timing_active, reverse=True)
        label = f"{float(threshold):.1f}+" if upper_bound is None else f"{float(threshold):.1f}-{float(upper_bound):.1f}"
        buckets.append(
            {
                "label": label,
                "threshold_s": round(float(threshold), 3),
                "upper_bound_s": round(float(upper_bound), 3) if upper_bound is not None else None,
                "count": len(items),
                "rate": round(len(items) / total, 4) if total else 0.0,
                "items": items,
            }
        )
    return buckets


def build_request_timing_summary(traces: Iterable[Dict[str, Any]], default_source_type: str = "") -> Dict[str, Any]:
    valid_traces = [_normalize_request_record(trace, default_source_type=default_source_type) for trace in traces if isinstance(trace, dict)]
    active_values = sorted(timing_active(trace) for trace in valid_traces)
    total_values = sorted(timing_total(trace) for trace in valid_traces)
    total = len(valid_traces)
    ok = sum(1 for trace in valid_traces if trace.get("status") == "ok")
    error = total - ok
    slow_bucket_stats = [
        {
            "label": bucket["label"],
            "threshold_s": bucket["threshold_s"],
            "upper_bound_s": bucket["upper_bound_s"],
            "count": bucket["count"],
            "rate": bucket["rate"],
        }
        for bucket in _build_threshold_buckets(valid_traces, default_source_type=default_source_type)
    ]
    return {
        "requests_total": total,
        "requests_ok": ok,
        "requests_error": error,
        "thresholds_s": [round(float(threshold), 3) for threshold in SLOW_REQUEST_THRESHOLDS],
        "elapsed_s": {
            "avg": round(sum(active_values) / total, 3) if total else 0.0,
            "min": round(min(active_values), 3) if active_values else 0.0,
            "max": round(max(active_values), 3) if active_values else 0.0,
            "p50": _quantile(active_values, 0.50),
            "p90": _quantile(active_values, 0.90),
            "p95": _quantile(active_values, 0.95),
        },
        "timing_s": {
            "active": {
                "avg": round(sum(active_values) / total, 3) if total else 0.0,
                "min": round(min(active_values), 3) if active_values else 0.0,
                "max": round(max(active_values), 3) if active_values else 0.0,
                "p50": _quantile(active_values, 0.50),
                "p90": _quantile(active_values, 0.90),
                "p95": _quantile(active_values, 0.95),
            },
            "total": {
                "avg": round(sum(total_values) / total, 3) if total else 0.0,
                "min": round(min(total_values), 3) if total_values else 0.0,
                "max": round(max(total_values), 3) if total_values else 0.0,
                "p50": _quantile(total_values, 0.50),
                "p90": _quantile(total_values, 0.90),
                "p95": _quantile(total_values, 0.95),
            },
        },
        "slow_request_buckets": slow_bucket_stats,
    }


def build_failed_items(traces: Iterable[Dict[str, Any]], default_source_type: str = "") -> List[Dict[str, Any]]:
    items = [
        _normalize_request_record(trace, default_source_type=default_source_type)
        for trace in traces
        if isinstance(trace, dict) and str(trace.get("status") or "ok") != "ok"
    ]
    items.sort(key=timing_active, reverse=True)
    return items


def build_slow_requests(traces: Iterable[Dict[str, Any]], default_source_type: str = "") -> Dict[str, Any]:
    buckets = _build_threshold_buckets(traces, default_source_type=default_source_type)
    total_count = sum(bucket["count"] for bucket in buckets)
    return {
        "thresholds_s": [round(float(threshold), 3) for threshold in SLOW_REQUEST_THRESHOLDS],
        "total_count": total_count,
        "buckets": buckets,
    }


def derive_status(calls_total: int, calls_ok: int, items: int) -> str:
    if calls_total <= 0:
        return "error"
    if calls_ok <= 0 or items <= 0:
        return "error"
    if calls_ok < calls_total:
        return "partial"
    return "ok"


def build_step_meta(
    *,
    step_key: str,
    status: str,
    elapsed_active_s: Optional[float] = None,
    elapsed_total_s: Optional[float] = None,
    items: int,
    calls_total: int,
    calls_ok: int,
    failed_items: Optional[List[Dict[str, Any]]] = None,
    request_traces: Iterable[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    traces = [_normalize_request_record(trace, default_source_type=step_key) for trace in request_traces if isinstance(trace, dict)]
    derived_failed_items = build_failed_items(traces, default_source_type=step_key)
    timing = normalize_timing(
        elapsed_active_s=elapsed_active_s,
        elapsed_total_s=elapsed_total_s,
    )
    return {
        "step_key": step_key,
        "status": status,
        "timing_s": timing,
        "items": int(items),
        "calls_total": int(calls_total),
        "calls_ok": int(calls_ok),
        "failed_calls": max(0, int(calls_total) - int(calls_ok)),
        "call_stats": {
            "kind": step_key,
            "total_calls": int(calls_total),
            "ok_calls": int(calls_ok),
            "failed_calls": max(0, int(calls_total) - int(calls_ok)),
        },
        "failed_items": derived_failed_items if failed_items is None else [
            _normalize_request_record(item, default_source_type=step_key)
            for item in failed_items
            if isinstance(item, dict)
        ],
        "request_timing_summary": build_request_timing_summary(traces, default_source_type=step_key),
        "slow_requests": build_slow_requests(traces, default_source_type=step_key),
    }


def build_meta(**kwargs: Any) -> Dict[str, Any]:
    return build_step_meta(**kwargs)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_result_with_meta(output_path: Path, result_payload: Dict[str, Any], meta_payload: Dict[str, Any]) -> Path:
    write_json(output_path, result_payload)
    meta_path = meta_path_for_output(output_path)
    write_json(meta_path, meta_payload)
    return meta_path
