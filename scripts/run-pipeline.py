#!/usr/bin/env python3
"""
news-hotspots 统一编排入口。

职责：
- 读取 runtime 配置
- 组织 fetch step 列表并并行执行 fetch phase
- 顺序执行 `merge-sources.py` 与 `merge-hotspots.py`
- 写入 step meta 与 `pipeline.meta.json`
- 归档各 step 的 meta，并清理过期 archive

执行逻辑：
1. 解析 CLI 参数并确定 debug / archive 目录
2. 并行执行各 fetch step，收集 `<step>.json` 与 `<step>.meta.json`
3. 执行 `merge-sources.py` 生成 `merge-sources.json`
4. 执行 `merge-hotspots.py` 生成最终热点 JSON / Markdown
5. 汇总所有 step 结果并写出 `pipeline.meta.json`

输出文件职责：
- `debug_dir/*.json`
  结果文件，给下游脚本消费
- `debug_dir/*.meta.json`
  诊断文件，给 pipeline 聚合和 `source-health.py` 消费
- `debug_dir/pipeline.meta.json`
  本次 pipeline 的总诊断汇总
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

try:
    from config_loader import load_merged_runtime_config
    from step_registry import ALL_SOURCE_STEPS, iter_fetch_steps
    from step_contract import local_now, local_today_iso, normalize_timing
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_registry import ALL_SOURCE_STEPS, iter_fetch_steps
    from step_contract import local_now, local_today_iso, normalize_timing

SCRIPTS_DIR = Path(__file__).parent
MERGE_STEP_KEY = "merge-sources"
HOTSPOTS_STEP_KEY = "merge-hotspots"
PROCESS_LOG_TAIL_LINES = 100


@dataclass(frozen=True)
class StepSpec:
    step_key: str
    name: str
    script_name: str
    args: List[str]
    output_path: Optional[Path]
    timeout_s: int


@dataclass
class ProcessResult:
    step_key: str
    name: str
    status: str
    elapsed_s: float
    timeout_s: int
    stdout_tail: List[str] = field(default_factory=list)
    stderr_tail: List[str] = field(default_factory=list)
    stdout_lines: int = 0
    stderr_lines: int = 0
    command: List[str] = field(default_factory=list)
    returncode: Optional[int] = None


class PipelineLogCapture(logging.Handler):
    def __init__(self, limit: int = PROCESS_LOG_TAIL_LINES):
        super().__init__()
        self.limit = max(1, limit)
        self._lines: List[str] = []
        self.total_lines = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            line = record.getMessage()
        self.total_lines += 1
        self._lines.append(line)
        if len(self._lines) > self.limit:
            self._lines = self._lines[-self.limit:]

    def snapshot(self) -> Dict[str, Any]:
        return {
            "line_count": self.total_lines,
            "tail": list(self._lines),
        }


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(__name__)


def status_icon(status: str) -> str:
    if status == "ok":
        return "✅"
    if status == "partial":
        return "⚠️"
    if status == "timeout":
        return "⏱️"
    return "❌"


def format_elapsed(elapsed_s: Any) -> str:
    try:
        return f"{float(elapsed_s):.1f}s"
    except (TypeError, ValueError):
        return "0.0s"


def format_timing_summary(active_s: Any, total_s: Any) -> str:
    active = format_elapsed(active_s)
    total = format_elapsed(total_s)
    if active == total:
        return total
    return f"active {active}, total {total}"


def summarize_items(summary: Dict[str, Any]) -> int:
    try:
        return int(summary.get("items", 0) or 0)
    except (TypeError, ValueError):
        return 0


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def load_json(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def summarize_stream(text: Optional[str], stream_name: str) -> tuple[List[str], int]:
    lines = [line.rstrip() for line in (text or "").splitlines() if line.strip()]
    return lines[-PROCESS_LOG_TAIL_LINES:], len(lines)


def build_process_logs(result: ProcessResult) -> Dict[str, Any]:
    tail: List[str] = []
    tail.extend(f"[stdout] {line}" for line in result.stdout_tail)
    tail.extend(f"[stderr] {line}" for line in result.stderr_tail)
    if len(tail) > PROCESS_LOG_TAIL_LINES:
        tail = tail[-PROCESS_LOG_TAIL_LINES:]
    summary = [
        f"status={result.status}",
        f"elapsed={format_elapsed(result.elapsed_s)}",
        f"timeout={result.timeout_s}s",
    ]
    if result.returncode is not None:
        summary.append(f"returncode={result.returncode}")
    if result.command:
        summary.append(f"command={' '.join(result.command)}")
    return {
        "summary": " | ".join(summary),
        "line_count": int(result.stdout_lines) + int(result.stderr_lines),
        "stdout_line_count": int(result.stdout_lines),
        "stderr_line_count": int(result.stderr_lines),
        "tail": tail,
    }


def normalize_meta_timing(meta_payload: Dict[str, Any], default_active: float, default_total: float) -> Dict[str, float]:
    timing = meta_payload.get("timing_s")
    if isinstance(timing, dict):
        return normalize_timing(
            timing.get("active", default_active),
            timing.get("total", default_total),
        )
    return normalize_timing(default_active, default_total)


def parse_output_markers(lines: Sequence[str]) -> Dict[str, str]:
    markers: Dict[str, str] = {}
    for line in lines:
        normalized_line = line
        if normalized_line.startswith("[stdout] ") or normalized_line.startswith("[stderr] "):
            normalized_line = normalized_line.split("] ", 1)[1]
        if "=" not in normalized_line:
            continue
        key, value = normalized_line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value:
            markers[key] = value
    return markers


def resolve_debug_dir(debug_dir: Optional[Path]) -> Path:
    if debug_dir is not None:
        debug_dir.mkdir(parents=True, exist_ok=True)
        return debug_dir
    return Path(tempfile.mkdtemp(prefix="news-hotspots-pipeline-"))


def cleanup_archive_root(archive_root: Path, retention_days: int) -> int:
    if not archive_root.exists():
        return 0
    cutoff = (local_now() - timedelta(days=retention_days)).date()
    removed = 0
    for child in archive_root.iterdir():
        if not child.is_dir():
            continue
        try:
            dir_date = datetime.strptime(child.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if dir_date < cutoff:
            shutil.rmtree(child, ignore_errors=True)
            removed += 1
    return removed


def archive_step_meta(step_meta_path: Path, archive_root: Path) -> Optional[Path]:
    if not step_meta_path.exists():
        return None
    date_dir = archive_root / local_today_iso() / "meta"
    date_dir.mkdir(parents=True, exist_ok=True)
    destination = date_dir / step_meta_path.name
    shutil.copy2(step_meta_path, destination)
    return destination


def load_runtime(defaults_dir: Path, config_dir: Optional[Path]) -> Dict[str, Any]:
    return load_merged_runtime_config(defaults_dir, config_dir)


def build_fetch_step_specs(
        defaults_dir: Path,
        config_dir: Optional[Path],
        debug_dir: Path,
        hours: int,
        verbose: bool,
        force: bool,
        runtime: Dict[str, Any],
) -> List[StepSpec]:
    timeout_s = int(runtime.get("pipeline", {}).get("fetch_step_timeout_s", 2000) or 2000)
    specs: List[StepSpec] = []
    for step in iter_fetch_steps():
        args = ["--defaults", str(defaults_dir)]
        if config_dir is not None:
            args.extend(["--config", str(config_dir)])
        args.extend(["--output", str(debug_dir / f"{step.step_key}.json"), "--hours", str(hours)])
        if verbose:
            args.append("--verbose")
        if force:
            args.append("--force")
        specs.append(
            StepSpec(
                step_key=step.step_key,
                name=step.display_name,
                script_name=step.script_name,
                args=args,
                output_path=debug_dir / f"{step.step_key}.json",
                timeout_s=timeout_s,
            )
        )
    return specs


def build_merge_step_spec(
        debug_dir: Path,
        archive_dir: Path,
        verbose: bool,
        runtime: Dict[str, Any],
) -> StepSpec:
    timeout_s = int(runtime.get("pipeline", {}).get("merge_timeout_s", 300) or 300)
    args = ["--output", str(debug_dir / "merge-sources.json"), "--archive", str(archive_dir)]
    for step in iter_fetch_steps():
        input_path = debug_dir / f"{step.step_key}.json"
        args.extend([step.merge_arg, str(input_path)])
    if verbose:
        args.append("--verbose")
    return StepSpec(
        step_key=MERGE_STEP_KEY,
        name="Merge Sources",
        script_name="merge-sources.py",
        args=args,
        output_path=debug_dir / "merge-sources.json",
        timeout_s=timeout_s,
    )


def build_hotspots_step_spec(
        debug_dir: Path,
        archive_dir: Path,
        mode: str,
        top_n: int,
        runtime: Dict[str, Any],
) -> StepSpec:
    timeout_s = int(runtime.get("pipeline", {}).get("hotspots_timeout_s", 120) or 120)
    return StepSpec(
        step_key=HOTSPOTS_STEP_KEY,
        name="Merge Hotspots",
        script_name="merge-hotspots.py",
        args=[
            "--input",
            str(debug_dir / "merge-sources.json"),
            "--archive",
            str(archive_dir),
            "--debug-output",
            str(debug_dir / "merge-hotspots.json"),
            "--top",
            str(top_n),
            "--mode",
            mode,
        ],
        output_path=None,
        timeout_s=timeout_s,
    )


def run_step_process(spec: StepSpec) -> ProcessResult:
    command = [sys.executable, str(SCRIPTS_DIR / spec.script_name), *spec.args]
    env = os.environ.copy()

    defaults_dir = _extract_cli_value(spec.args, "--defaults")
    if defaults_dir:
        env["NEWS_HOTSPOTS_DEFAULTS_DIR"] = defaults_dir
    config_dir = _extract_cli_value(spec.args, "--config")
    if config_dir:
        env["NEWS_HOTSPOTS_CONFIG_DIR"] = config_dir

    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=spec.timeout_s,
            env=env,
        )
        elapsed_s = round(time.monotonic() - started, 3)
        stdout_tail, stdout_lines = summarize_stream(completed.stdout, "stdout")
        stderr_tail, stderr_lines = summarize_stream(completed.stderr, "stderr")
        status = "ok" if completed.returncode == 0 else "error"
        return ProcessResult(
            step_key=spec.step_key,
            name=spec.name,
            status=status,
            elapsed_s=elapsed_s,
            timeout_s=spec.timeout_s,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            stdout_lines=stdout_lines,
            stderr_lines=stderr_lines,
            command=command,
            returncode=completed.returncode,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed_s = round(time.monotonic() - started, 3)
        stdout_tail, stdout_lines = summarize_stream(exc.stdout, "stdout")
        stderr_tail, stderr_lines = summarize_stream(exc.stderr, "stderr")
        stderr_tail.append(f"Killed after {spec.timeout_s}s")
        return ProcessResult(
            step_key=spec.step_key,
            name=spec.name,
            status="timeout",
            elapsed_s=elapsed_s,
            timeout_s=spec.timeout_s,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            stdout_lines=stdout_lines,
            stderr_lines=stderr_lines + 1,
            command=command,
            returncode=None,
        )
    except Exception as exc:
        elapsed_s = round(time.monotonic() - started, 3)
        return ProcessResult(
            step_key=spec.step_key,
            name=spec.name,
            status="error",
            elapsed_s=elapsed_s,
            timeout_s=spec.timeout_s,
            stderr_tail=[str(exc)],
            stderr_lines=1,
            command=command,
            returncode=None,
        )


def _extract_cli_value(args: Sequence[str], flag: str) -> Optional[str]:
    try:
        index = list(args).index(flag)
    except ValueError:
        return None
    if index + 1 >= len(args):
        return None
    return str(args[index + 1])


def build_simple_meta(
        step_key: str,
        result: ProcessResult,
        items: int = 0,
        calls_total: int = 0,
        calls_ok: int = 0,
        failed_items: Optional[List[Dict[str, Any]]] = None,
        output_path: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    timing = normalize_timing(result.elapsed_s, result.elapsed_s)
    payload: Dict[str, Any] = {
        "step_key": step_key,
        "status": result.status,
        "timing_s": timing,
        "items": items,
        "calls_total": calls_total,
        "calls_ok": calls_ok,
        "failed_calls": max(0, calls_total - calls_ok),
        "call_stats": {
            "kind": step_key,
            "total_calls": calls_total,
            "ok_calls": calls_ok,
            "failed_calls": max(0, calls_total - calls_ok),
        },
        "failed_items": failed_items or [],
        "timeout_s": result.timeout_s,
        "logs": build_process_logs(result),
    }
    if output_path:
        payload["output_path"] = output_path
    if extra:
        payload.update(extra)
    return payload


def fetch_step_meta_path(output_path: Path) -> Path:
    return output_path.with_suffix(".meta.json")


def summarize_fetch_step(spec: StepSpec, result: ProcessResult) -> Dict[str, Any]:
    output_payload = load_json(spec.output_path)
    meta_payload = load_json(fetch_step_meta_path(spec.output_path)) if spec.output_path else None
    if meta_payload:
        meta_payload.setdefault("step_key", spec.step_key)
        timing = normalize_meta_timing(meta_payload, result.elapsed_s, result.elapsed_s)
        meta_payload["timing_s"] = timing
        meta_payload.setdefault("timeout_s", result.timeout_s)
        meta_payload.setdefault("status", result.status)
        meta_payload.setdefault("output_path", str(spec.output_path) if spec.output_path else "")
        meta_payload.setdefault("call_stats", {
            "kind": spec.step_key,
            "total_calls": int(meta_payload.get("calls_total", 0) or 0),
            "ok_calls": int(meta_payload.get("calls_ok", 0) or 0),
            "failed_calls": int(meta_payload.get("failed_calls", 0) or 0),
        })
        meta_payload["logs"] = build_process_logs(result)
        return meta_payload

    items = len(output_payload.get("articles", [])) if output_payload else 0
    status = "ok" if result.status == "ok" and items > 0 else result.status
    return build_simple_meta(
        step_key=spec.step_key,
        result=ProcessResult(**{**result.__dict__, "status": status}),
        items=items,
        calls_total=1 if spec.output_path and spec.output_path.exists() else 0,
        calls_ok=1 if status == "ok" else 0,
        output_path=str(spec.output_path) if spec.output_path else None,
    )


def run_fetch_phase(
        logger: logging.Logger,
        fetch_specs: Sequence[StepSpec],
        skipped: set[str],
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, str], float]:
    step_summaries: Dict[str, Dict[str, Any]] = {}
    outputs: Dict[str, str] = {}
    active_specs = [spec for spec in fetch_specs if spec.step_key not in skipped]
    max_workers = max(1, len(active_specs))
    started_at = time.monotonic()

    for spec in fetch_specs:
        if spec.step_key in skipped:
            logger.info("  ⏭️ %s: skipped", spec.name)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_spec = {
            executor.submit(run_step_process, spec): spec
            for spec in active_specs
        }
        for future in as_completed(future_to_spec):
            spec = future_to_spec[future]
            result = future.result()
            summary = summarize_fetch_step(spec, result)
            step_summaries[spec.step_key] = summary
            if spec.output_path:
                write_json(fetch_step_meta_path(spec.output_path), summary)
            if spec.output_path:
                outputs[spec.step_key] = str(spec.output_path)
            logger.info(
                "  %s %s: %d items (%s)",
                status_icon(summary.get("status", result.status)),
                spec.name,
                summarize_items(summary),
                format_timing_summary(
                    (summary.get("timing_s", {}) if isinstance(summary.get("timing_s"), dict) else {}).get("active", result.elapsed_s),
                    (summary.get("timing_s", {}) if isinstance(summary.get("timing_s"), dict) else {}).get("total", result.elapsed_s),
                ),
            )

    return step_summaries, outputs, round(time.monotonic() - started_at, 3)


def summarize_merge_step(spec: StepSpec, result: ProcessResult) -> Dict[str, Any]:
    payload = load_json(spec.output_path)
    items = int(payload.get("output_stats", {}).get("total_articles", 0) or 0) if payload else 0
    input_items = int(payload.get("input_stats", {}).get("total_articles", 0) or 0) if payload else 0
    status = "ok" if result.status == "ok" and payload else result.status
    meta = build_simple_meta(
        step_key=spec.step_key,
        result=ProcessResult(**{**result.__dict__, "status": status}),
        items=items,
        calls_total=input_items,
        calls_ok=items,
        output_path=str(spec.output_path) if spec.output_path else None,
        extra={"input_items": input_items},
    )
    write_json(fetch_step_meta_path(spec.output_path), meta)
    return meta


def build_pipeline_meta(
        runtime: Dict[str, Any],
        step_summaries: Dict[str, Dict[str, Any]],
        outputs: Dict[str, str],
        archive_root: Path,
        cleaned_archives: int,
        started_at: float,
        fetch_elapsed_s: float = 0.0,
        pipeline_logs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    statuses = [summary.get("status", "error") for summary in step_summaries.values()]
    if outputs.get("hotspots_output"):
        overall_status = "ok"
    elif any(status == "ok" for status in statuses):
        overall_status = "partial"
    else:
        overall_status = "error"

    source_type_overview: Dict[str, Dict[str, Any]] = {}
    fetch_active_s = 0.0
    for step in ALL_SOURCE_STEPS:
        summary = step_summaries.get(step.step_key)
        if not isinstance(summary, dict):
            continue
        step_timing = normalize_meta_timing(summary, 0.0, 0.0)
        fetch_active_s += step_timing["active"]
        failed_items = summary.get("failed_items", [])
        slow_requests = summary.get("slow_requests", {})
        source_type_overview[step.step_key] = {
            "status": summary.get("status", "error"),
            "fully_successful": summary.get("status") == "ok",
            "items": int(summary.get("items", 0) or 0),
            "timing_s": step_timing,
            "calls_total": int(summary.get("calls_total", 0) or 0),
            "calls_ok": int(summary.get("calls_ok", 0) or 0),
            "failed_calls": int(summary.get("failed_calls", 0) or 0),
            "failed_items_count": len(failed_items) if isinstance(failed_items, list) else 0,
            "slow_requests_count": int(slow_requests.get("total_count", 0) or 0) if isinstance(slow_requests, dict) else 0,
        }

    pipeline_timing = normalize_timing(fetch_active_s, time.monotonic() - started_at)
    fetch_timing = normalize_timing(fetch_active_s, fetch_elapsed_s)
    steps = [
        {
            "step_key": step_key,
            "name": summary.get("name", step_key),
            "status": summary.get("status", "error"),
            "timing_s": normalize_meta_timing(summary, 0.0, 0.0),
            "items": int(summary.get("items", 0) or 0),
            "logs": summary.get("logs", {}),
        }
        for step_key, summary in step_summaries.items()
        if isinstance(summary, dict)
    ]
    total_items = 0
    if isinstance(step_summaries.get(HOTSPOTS_STEP_KEY), dict):
        total_items = int(step_summaries[HOTSPOTS_STEP_KEY].get("items", 0) or 0)
    elif isinstance(step_summaries.get(MERGE_STEP_KEY), dict):
        total_items = int(step_summaries[MERGE_STEP_KEY].get("items", 0) or 0)
    return {
        "pipeline_version": "3.0.0",
        "generated_at": local_now().isoformat(),
        "status": overall_status,
        "overall_status": overall_status,
        "timing_s": pipeline_timing,
        "fetch_active_elapsed_s": fetch_timing["active"],
        "fetch_timing_s": fetch_timing,
        "items": total_items,
        "call_stats": {
            "kind": "steps",
            "total_calls": len(step_summaries),
            "ok_calls": sum(1 for summary in step_summaries.values() if isinstance(summary, dict) and summary.get("status") == "ok"),
            "failed_calls": sum(1 for summary in step_summaries.values() if isinstance(summary, dict) and summary.get("status") in {"error", "timeout"}),
        },
        "cooldown_s": {step.step_key: runtime.get("fetch", {}).get(step.step_key, {}).get("cooldown_s", 0) for step in ALL_SOURCE_STEPS},
        "source_type_overview": source_type_overview,
        "step_summaries": step_summaries,
        "steps": steps,
        "hotspots_output": outputs.get("hotspots_output"),
        "markdown_output": outputs.get("markdown_output"),
        "merged_output": outputs.get("merged_output"),
        "archive_root": str(archive_root),
        "cleaned_archives": cleaned_archives,
        "logs": pipeline_logs or {"line_count": 0, "tail": []},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the sequential news-hotspots pipeline.")
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Defaults config directory. Default: config/defaults")
    parser.add_argument("--config", type=Path, default=Path("workspace/config"), help="Workspace config directory. Default: workspace/config")
    parser.add_argument("--archive", type=Path, default=Path("workspace/archive/news-hotspots"), help="Archive root directory for final outputs and meta files. Default: workspace/archive/news-hotspots")
    parser.add_argument("--debug", type=Path, default=None, help="Debug output directory for step JSON and pipeline meta. Default: auto-created temp dir")
    parser.add_argument("--mode", choices=["daily", "weekly"], default="daily", help="Hotspots output mode. Default: daily")
    parser.add_argument("--hours", type=int, default=48, help="Time window in hours passed to fetch steps. Default: 48")
    parser.add_argument("--top", type=int, default=None, help="Override hotspots top N. Default: runtime pipeline.default_hotspots_top_n")
    parser.add_argument("--skip", type=str, default="", help="Comma-separated fetch step keys to skip, for example: rss,google,twitter")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", action="store_true", help="Pass --force to fetch steps")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    pipeline_log_capture = PipelineLogCapture()
    pipeline_log_capture.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
    logger.addHandler(pipeline_log_capture)
    debug_dir = resolve_debug_dir(args.debug)
    config_dir = args.config if args.config.exists() else None
    runtime = load_runtime(args.defaults, config_dir)
    archive_root = args.archive
    archive_root.mkdir(parents=True, exist_ok=True)
    started_at = time.monotonic()
    skipped = {item.strip() for item in args.skip.split(",") if item.strip()}

    logger.info("📁 Debug directory: %s", debug_dir)
    logger.info("🗄️ Archive directory: %s", archive_root)
    logger.info("🗂️ Hotspots mode: %s", args.mode)
    if config_dir is None:
        logger.info("ℹ️ Config overlay not found, using defaults only: %s", args.config)
    else:
        logger.info("🧩 Config overlay: %s", config_dir)

    cleaned_archives = cleanup_archive_root(
        archive_root,
        int(runtime.get("pipeline", {}).get("archive_retention_days", 90) or 90),
    )
    top_n = args.top if args.top is not None else int(runtime.get("pipeline", {}).get("default_hotspots_top_n", 5) or 5)

    fetch_specs = build_fetch_step_specs(args.defaults, config_dir, debug_dir, args.hours, args.verbose, args.force,
                                         runtime)
    active_fetch_specs = [spec for spec in fetch_specs if spec.step_key not in skipped]

    logger.info("🚀 Starting pipeline: %d/%d sources, %sh window", len(active_fetch_specs), len(ALL_SOURCE_STEPS), args.hours)
    step_summaries, outputs, fetch_elapsed_s = run_fetch_phase(logger, fetch_specs, skipped)
    logger.info("📡 Fetch phase done in %s", format_elapsed(fetch_elapsed_s))

    merge_spec = build_merge_step_spec(debug_dir, archive_root, args.verbose, runtime)
    merge_result = run_step_process(merge_spec)
    merge_summary = summarize_merge_step(merge_spec, merge_result)
    step_summaries[merge_spec.step_key] = merge_summary
    if merge_spec.output_path and merge_spec.output_path.exists():
        outputs["merged_output"] = str(merge_spec.output_path)
    logger.info(
        "  %s Merge: %d items (%s)",
        status_icon(merge_summary.get("status", merge_result.status)),
        summarize_items(merge_summary),
        format_timing_summary(
            (merge_summary.get("timing_s", {}) if isinstance(merge_summary.get("timing_s"), dict) else {}).get("active", merge_result.elapsed_s),
            (merge_summary.get("timing_s", {}) if isinstance(merge_summary.get("timing_s"), dict) else {}).get("total", merge_result.elapsed_s),
        ),
    )

    hotspots_spec = build_hotspots_step_spec(debug_dir, archive_root, args.mode, top_n, runtime)
    hotspots_result = run_step_process(hotspots_spec)
    hotspots_markers = parse_output_markers(hotspots_result.stdout_tail)
    hotspots_summary = build_simple_meta(
        step_key=hotspots_spec.step_key,
        result=hotspots_result,
        items=sum(
            len(topic.get("items", []))
            for topic in (load_json(Path(hotspots_markers["ARCHIVED_JSON"])) or {}).get("topics", [])
            if isinstance(topic, dict)
        ) if "ARCHIVED_JSON" in hotspots_markers else 0,
        calls_total=1 if "ARCHIVED_JSON" in hotspots_markers else 0,
        calls_ok=1 if hotspots_result.status == "ok" and "ARCHIVED_JSON" in hotspots_markers else 0,
        output_path=hotspots_markers.get("ARCHIVED_JSON"),
        extra={
            "markdown_output": hotspots_markers.get("ARCHIVED_MARKDOWN"),
            "merged_output": hotspots_markers.get("ARCHIVED_MERGED_JSON"),
        },
    )
    write_json(debug_dir / "merge-hotspots.meta.json", hotspots_summary)
    step_summaries[hotspots_spec.step_key] = hotspots_summary
    if "ARCHIVED_JSON" in hotspots_markers:
        outputs["hotspots_output"] = hotspots_markers["ARCHIVED_JSON"]
    if "ARCHIVED_MARKDOWN" in hotspots_markers:
        outputs["markdown_output"] = hotspots_markers["ARCHIVED_MARKDOWN"]
    if "ARCHIVED_MERGED_JSON" in hotspots_markers:
        outputs["merged_output"] = hotspots_markers["ARCHIVED_MERGED_JSON"]
    logger.info(
        "  %s Hotspots: %d items (%s)",
        status_icon(hotspots_summary.get("status", hotspots_result.status)),
        summarize_items(hotspots_summary),
        format_timing_summary(
            (hotspots_summary.get("timing_s", {}) if isinstance(hotspots_summary.get("timing_s"), dict) else {}).get("active", hotspots_result.elapsed_s),
            (hotspots_summary.get("timing_s", {}) if isinstance(hotspots_summary.get("timing_s"), dict) else {}).get("total", hotspots_result.elapsed_s),
        ),
    )

    pipeline_meta = build_pipeline_meta(
        runtime,
        step_summaries,
        outputs,
        archive_root,
        cleaned_archives,
        started_at,
        fetch_elapsed_s,
        pipeline_logs=pipeline_log_capture.snapshot(),
    )
    pipeline_meta_path = debug_dir / "pipeline.meta.json"
    write_json(pipeline_meta_path, pipeline_meta)

    archived_pipeline_meta = archive_step_meta(pipeline_meta_path, archive_root)
    archived_step_meta_paths: Dict[str, str] = {}
    for step_key in list(step_summaries.keys()):
        meta_path = debug_dir / f"{step_key}.meta.json"
        archived = archive_step_meta(meta_path, archive_root)
        if archived:
            archived_step_meta_paths[step_key] = str(archived)
    if archived_pipeline_meta:
        pipeline_meta["archived_pipeline_meta"] = str(archived_pipeline_meta)
    if archived_step_meta_paths:
        pipeline_meta["archived_step_meta_paths"] = archived_step_meta_paths
    pipeline_meta["logs"] = pipeline_log_capture.snapshot()
    write_json(pipeline_meta_path, pipeline_meta)

    if outputs.get("hotspots_output") or outputs.get("markdown_output"):
        logger.info("🗂️ Archived files:")
        if outputs.get("hotspots_output"):
            logger.info("   Hotspots JSON: %s", outputs["hotspots_output"])
        if outputs.get("markdown_output"):
            logger.info("   Markdown: %s", outputs["markdown_output"])
        if archived_step_meta_paths:
            sample_meta = next(iter(archived_step_meta_paths.values()))
            logger.info("   Meta: %s", Path(sample_meta).parent)
    if cleaned_archives:
        logger.info("🧹 Cleaned %d expired archive directories", cleaned_archives)
    if outputs.get("markdown_output"):
        logger.info("✅ Done → %s", outputs["markdown_output"])
    else:
        logger.info("%s Done with status=%s", status_icon(pipeline_meta["status"]), pipeline_meta["status"])
    return 0 if pipeline_meta["status"] in {"ok", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
