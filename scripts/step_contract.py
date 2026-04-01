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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

SLOW_REQUEST_THRESHOLDS = (3.0, 5.0, 10.0)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def normalize_failed_item(item_id: Any, error: Any, elapsed_s: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "id": str(item_id or "item").strip() or "item",
        "error": " ".join(str(error or "").split())[:300],
    }
    try:
        payload["elapsed_s"] = round(float(elapsed_s or 0), 3)
    except (TypeError, ValueError):
        payload["elapsed_s"] = 0.0
    return payload


def meta_path_for_output(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}.meta.json")


def build_request_trace(request_id: Any, target: Any = None, elapsed_s: Any = None, status: str = "ok", error: Any = None, **extra: Any) -> Dict[str, Any]:
    if elapsed_s is None:
        elapsed_s = target
        target = request_id
    trace: Dict[str, Any] = {
        "id": str(request_id or target or "request").strip() or "request",
        "target": str(target or request_id or "").strip(),
        "status": str(status or "ok"),
        "elapsed_s": round(float(elapsed_s or 0), 3),
    }
    if error not in (None, ""):
        trace["error"] = " ".join(str(error).split())[:300]
    trace.update(extra)
    return trace


def _threshold_key(threshold: float) -> str:
    return f"{float(threshold):.1f}"


def build_timing_summary(traces: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    summary = {
        "requests_total": 0,
        "requests_ok": 0,
        "requests_error": 0,
        "slow_by_threshold": {_threshold_key(threshold): 0 for threshold in SLOW_REQUEST_THRESHOLDS},
    }
    for trace in traces:
        if not isinstance(trace, dict):
            continue
        summary["requests_total"] += 1
        if trace.get("status") == "ok":
            summary["requests_ok"] += 1
        else:
            summary["requests_error"] += 1
        elapsed_s = float(trace.get("elapsed_s", 0) or 0)
        for threshold in SLOW_REQUEST_THRESHOLDS:
            if elapsed_s >= threshold:
                summary["slow_by_threshold"][_threshold_key(threshold)] += 1
    return summary


def build_slow_requests(traces: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets = {_threshold_key(threshold): [] for threshold in SLOW_REQUEST_THRESHOLDS}
    for trace in traces:
        if not isinstance(trace, dict):
            continue
        elapsed_s = float(trace.get("elapsed_s", 0) or 0)
        for threshold in SLOW_REQUEST_THRESHOLDS:
            if elapsed_s >= threshold:
                buckets[_threshold_key(threshold)].append(dict(trace))
    return buckets


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
    elapsed_s: float,
    items: int,
    calls_total: int,
    calls_ok: int,
    failed_items: List[Dict[str, Any]],
    request_traces: Iterable[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    traces = [trace for trace in request_traces if isinstance(trace, dict)]
    return {
        "step_key": step_key,
        "status": status,
        "elapsed_s": round(float(elapsed_s), 3),
        "items": int(items),
        "calls_total": int(calls_total),
        "calls_ok": int(calls_ok),
        "failed_calls": max(0, int(calls_total) - int(calls_ok)),
        "failed_items": failed_items,
        "timing_summary": build_timing_summary(traces),
        "slow_requests": build_slow_requests(traces),
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
