#!/usr/bin/env python3
"""
Unified data collection pipeline for news-hotspots.

Runs fetch steps, merges them into an internal JSON inside a debug directory,
then renders a compact hotspots JSON for downstream prompt-writing flows.
"""

import argparse
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

SCRIPTS_DIR = Path(__file__).parent
DEFAULT_TIMEOUT = 2000
MERGE_TIMEOUT = 300
SUMMARY_TIMEOUT = 120
DEFAULT_HOTSPOTS_TOP = 5
ARCHIVE_RETENTION_DAYS = 90
ERROR_TEXT_LIMIT = 180
STEP_COOLDOWN_DEFAULTS = {
    "fetch-twitter.py": ("BB_BROWSER_TWITTER_COOLDOWN_SECONDS", 15.0),
    "fetch-reddit.py": ("BB_BROWSER_REDDIT_COOLDOWN_SECONDS", 8.0),
    "fetch-google.py": ("BB_BROWSER_GOOGLE_COOLDOWN_SECONDS", 10.0),
    "fetch-v2ex.py": ("BB_BROWSER_V2EX_COOLDOWN_SECONDS", 8.0),
    "fetch-github.py": ("NEWS_HOTSPOTS_GITHUB_COOLDOWN_SECONDS", 6.0),
    "fetch-github-trending.py": ("NEWS_HOTSPOTS_GITHUB_TRENDING_COOLDOWN_SECONDS", 6.0),
}

INTERRUPT_EVENT = threading.Event()
INTERRUPT_REASON = "external interrupt"


@dataclass(frozen=True)
class StepSpec:
    step_key: str
    name: str
    script: str
    args: List[str]
    output_path: Optional[Path]
    cooldown_s: Optional[float] = None


@dataclass
class ProcessResult:
    step_key: str
    name: str
    status: str
    elapsed_s: float
    effective_timeout_s: int
    cooldown_s: Optional[float]
    stderr_tail: List[str] = field(default_factory=list)
    stdout_tail: List[str] = field(default_factory=list)


@dataclass
class StepMeta:
    status: str
    items: int
    call_stats: Dict[str, Any]
    failed_items: List[Dict[str, str]]
    details: Dict[str, Any]


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(__name__)


def note_interrupt(reason: str) -> None:
    global INTERRUPT_REASON
    if not INTERRUPT_EVENT.is_set():
        INTERRUPT_REASON = reason
    INTERRUPT_EVENT.set()


def install_signal_handlers(logger: logging.Logger) -> Dict[int, Any]:
    previous: Dict[int, Any] = {}

    def handler(signum: int, _frame: Any) -> None:
        signal_name = signal.Signals(signum).name if signum in signal.Signals._value2member_map_ else str(signum)
        note_interrupt(f"received {signal_name}")
        logger.warning("⚠️ Received %s, will stop active fetches and try to recover partial outputs", signal_name)

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, handler)
    return previous


def restore_signal_handlers(previous: Dict[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def load_json_file(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def normalize_error_text(value: Any, limit: int = ERROR_TEXT_LIMIT) -> str:
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


def get_cooldown_for_script(script: str) -> Optional[float]:
    config = STEP_COOLDOWN_DEFAULTS.get(script)
    if not config:
        return None
    env_name, default = config
    try:
        return float(os.environ.get(env_name, str(default)))
    except ValueError:
        return default


def resolve_debug_dir(debug_dir: Optional[Path]) -> Path:
    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)
        return debug_dir
    return Path(tempfile.mkdtemp(prefix="news-hotspots-pipeline-"))


def resolve_unique_output_path(path: Path) -> Path:
    if not path.exists():
        return path
    parent = path.parent
    stem = path.stem
    suffix = path.suffix
    counter = 1
    while True:
        candidate = parent / f"{stem}{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def cleanup_archive_root(archive_root: Path, retention_days: int = ARCHIVE_RETENTION_DAYS) -> int:
    if not archive_root.exists():
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).date()
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


def archive_meta_outputs(
    archive_root: Optional[Path],
    pipeline_meta_output: Path,
    step_meta_paths: Dict[str, str],
) -> Dict[str, Any]:
    if not archive_root:
        return {}

    today_dir = archive_root / datetime.now(timezone.utc).date().isoformat()
    meta_dir = today_dir / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)

    archived: Dict[str, Any] = {
        "date_dir": str(today_dir),
        "meta_dir": str(meta_dir),
    }

    if pipeline_meta_output.exists():
        archived_pipeline_meta = resolve_unique_output_path(meta_dir / pipeline_meta_output.name)
        shutil.copy2(pipeline_meta_output, archived_pipeline_meta)
        archived["pipeline_meta"] = str(archived_pipeline_meta)

    archived_step_metas: Dict[str, str] = {}
    for step_key, path_str in step_meta_paths.items():
        source_path = Path(path_str)
        if not source_path.exists():
            continue
        archived_path = resolve_unique_output_path(meta_dir / source_path.name)
        shutil.copy2(source_path, archived_path)
        archived_step_metas[step_key] = str(archived_path)
    archived["step_meta_paths"] = archived_step_metas
    return archived


def parse_step_output_paths(stdout_tail: Sequence[str]) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for line in stdout_tail:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value:
            parsed[key] = value
    return parsed


def extract_items_from_payload(payload: Optional[Dict[str, Any]], fallback: int = 0) -> int:
    if not payload:
        return int(fallback or 0)
    return int(
        payload.get("items_total")
        or payload.get("total_articles")
        or payload.get("total_posts")
        or payload.get("total_releases")
        or payload.get("total_results")
        or payload.get("total")
        or payload.get("output_stats", {}).get("total_articles")
        or fallback
        or 0
    )


def collect_payload_details(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not payload:
        return {}

    details: Dict[str, Any] = {}
    for key in [
        "generated",
        "source_type",
        "total_articles",
        "total_posts",
        "total_releases",
        "total_results",
        "total",
    ]:
        if key in payload:
            details[key] = payload[key]

    if "sources" in payload and isinstance(payload["sources"], list):
        statuses = [source.get("status", "error") for source in payload["sources"] if isinstance(source, dict)]
        details["record_summary"] = {
            "kind": "sources",
            "total": len(statuses),
            "ok": sum(1 for status in statuses if status == "ok"),
            "error": sum(1 for status in statuses if status != "ok"),
        }
    elif "subreddits" in payload and isinstance(payload["subreddits"], list):
        statuses = [entry.get("status", "error") for entry in payload["subreddits"] if isinstance(entry, dict)]
        details["record_summary"] = {
            "kind": "subreddits",
            "total": len(statuses),
            "ok": sum(1 for status in statuses if status == "ok"),
            "error": sum(1 for status in statuses if status != "ok"),
        }
    elif "topics" in payload and isinstance(payload["topics"], list):
        topic_entries = [entry for entry in payload["topics"] if isinstance(entry, dict)]
        if any("status" in entry for entry in topic_entries):
            statuses = [entry.get("status", "error") for entry in topic_entries]
            details["record_summary"] = {
                "kind": "topics",
                "total": len(statuses),
                "ok": sum(1 for status in statuses if status == "ok"),
                "error": sum(1 for status in statuses if status != "ok"),
            }
        else:
            details["record_summary"] = {
                "kind": "topics",
                "total": len(topic_entries),
                "ok": len(topic_entries),
                "error": 0,
            }
    elif "repos" in payload and isinstance(payload["repos"], list):
        details["record_summary"] = {
            "kind": "repos",
            "total": len(payload["repos"]),
            "ok": len(payload["repos"]),
            "error": 0,
        }

    if "processing" in payload:
        details["processing"] = payload["processing"]
    if "input_sources" in payload:
        details["input_sources"] = payload["input_sources"]
    if "output_stats" in payload:
        details["output_stats"] = payload["output_stats"]
        total_input = payload.get("input_sources", {}).get("total_input")
        total_output = payload.get("output_stats", {}).get("total_articles")
        if isinstance(total_input, int) and isinstance(total_output, int) and total_input >= 0:
            dropped = max(total_input - total_output, 0)
            details["deduplication"] = {
                "input_total": total_input,
                "output_total": total_output,
                "dropped": dropped,
                "drop_ratio": round(dropped / total_input, 3) if total_input else 0.0,
            }
    if "topic_order" in payload and isinstance(payload["topic_order"], list):
        details["hotspots_stats"] = {
            "total_articles": payload.get("total_articles", 0),
            "topic_count": len(payload["topic_order"]),
            "topic_order": payload["topic_order"],
            "source_breakdown": payload.get("source_breakdown", {}),
        }

    return details


def extract_call_stats(payload: Optional[Dict[str, Any]], *, step_key: str, status: str) -> Dict[str, Any]:
    def single_call(kind: str) -> Dict[str, Any]:
        if status in {"pending", "skipped"}:
            return {"kind": kind, "total_calls": 0, "ok_calls": 0, "failed_calls": 0}
        ok_calls = 1 if status == "ok" else 0
        return {"kind": kind, "total_calls": 1, "ok_calls": ok_calls, "failed_calls": 1 - ok_calls}

    if not payload:
        return single_call(step_key)

    if any(key in payload for key in ("calls_total", "calls_ok")):
        total_calls = int(payload.get("calls_total", 0) or 0)
        ok_calls = int(payload.get("calls_ok", 0) or 0)
        return {
            "kind": str(payload.get("calls_kind", step_key)),
            "total_calls": total_calls,
            "ok_calls": ok_calls,
            "failed_calls": max(total_calls - ok_calls, 0),
        }

    if isinstance(payload.get("sources"), list):
        entries = [entry for entry in payload["sources"] if isinstance(entry, dict)]
        ok_calls = sum(1 for entry in entries if entry.get("status") == "ok")
        return {"kind": "sources", "total_calls": len(entries), "ok_calls": ok_calls, "failed_calls": max(len(entries) - ok_calls, 0)}

    if isinstance(payload.get("subreddits"), list):
        entries = [entry for entry in payload["subreddits"] if isinstance(entry, dict)]
        ok_calls = sum(1 for entry in entries if entry.get("status") == "ok")
        return {"kind": "subreddits", "total_calls": len(entries), "ok_calls": ok_calls, "failed_calls": max(len(entries) - ok_calls, 0)}

    if isinstance(payload.get("topics"), list):
        entries = [entry for entry in payload["topics"] if isinstance(entry, dict)]
        query_stats = [
            stat
            for entry in entries
            for stat in entry.get("query_stats", [])
            if isinstance(stat, dict)
        ]
        if query_stats:
            ok_calls = sum(1 for stat in query_stats if stat.get("status") == "ok")
            return {"kind": "queries", "total_calls": len(query_stats), "ok_calls": ok_calls, "failed_calls": max(len(query_stats) - ok_calls, 0)}
        if any("status" in entry for entry in entries):
            ok_calls = sum(1 for entry in entries if entry.get("status") == "ok")
            return {"kind": "topics", "total_calls": len(entries), "ok_calls": ok_calls, "failed_calls": max(len(entries) - ok_calls, 0)}

    if isinstance(payload.get("repos"), list):
        entries = [entry for entry in payload["repos"] if isinstance(entry, dict)]
        if any("status" in entry for entry in entries):
            ok_calls = sum(1 for entry in entries if entry.get("status") == "ok")
            return {"kind": "repos", "total_calls": len(entries), "ok_calls": ok_calls, "failed_calls": max(len(entries) - ok_calls, 0)}
        return single_call("repos")

    return single_call(step_key)


def extract_failed_items(payload: Optional[Dict[str, Any]], limit: int = 10) -> List[Dict[str, str]]:
    if not payload:
        return []

    failed_items: List[Dict[str, str]] = []

    def append_item(entry_id: Any, error_value: Any) -> None:
        item_id = str(entry_id).strip() if entry_id is not None else ""
        error_text = normalize_error_text(error_value)
        if not error_text:
            return
        if not item_id:
            item_id = "item"
        candidate = {"id": item_id, "error": error_text}
        if candidate not in failed_items:
            failed_items.append(candidate)

    for key in ("sources", "subreddits", "topics", "repos"):
        entries = payload.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            status = entry.get("status")
            explicit_error = entry.get("error")
            error_messages = entry.get("error_messages")
            has_error = bool(explicit_error) or (
                isinstance(error_messages, list) and any(str(item).strip() for item in error_messages)
            )
            if status == "ok" or (status is None and not has_error):
                if key != "topics":
                    continue
            entry_id = (
                entry.get("source_id")
                or entry.get("id")
                or entry.get("subreddit")
                or entry.get("topic")
                or entry.get("repo")
                or entry.get("name")
                or entry.get("query")
                or entry.get("topic_id")
            )
            if key == "topics" and isinstance(entry.get("query_stats"), list):
                topic_query_failures = False
                for query_stat in entry["query_stats"]:
                    if not isinstance(query_stat, dict) or query_stat.get("status") == "ok":
                        continue
                    topic_query_failures = True
                    append_item(entry_id or entry.get("topic_id") or entry.get("topic"), query_stat.get("error") or query_stat.get("query"))
                    if len(failed_items) >= limit:
                        return failed_items[:limit]
                if status == "ok" and topic_query_failures:
                    continue
            if explicit_error:
                append_item(entry_id, explicit_error)
            elif isinstance(error_messages, list) and error_messages:
                first_message = next((item for item in error_messages if normalize_error_text(item)), None)
                append_item(entry_id, first_message)
            if len(failed_items) >= limit:
                return failed_items[:limit]

    return failed_items[:limit]


def build_diagnostics(payload: Optional[Dict[str, Any]], process_result: ProcessResult, step_key: str) -> StepMeta:
    details = collect_payload_details(payload)
    items = extract_items_from_payload(payload)
    call_stats = extract_call_stats(payload, step_key=step_key, status=process_result.status)
    failed_items = extract_failed_items(payload)
    if not failed_items and process_result.status in {"error", "timeout"}:
        message = next((normalize_error_text(line) for line in process_result.stderr_tail if normalize_error_text(line)), process_result.status)
        failed_items = [{"id": "__step__", "error": message}]
    return StepMeta(
        status=process_result.status,
        items=items,
        call_stats=call_stats,
        failed_items=failed_items,
        details=details,
    )


def serialize_step_meta(spec: StepSpec, process_result: ProcessResult, meta: StepMeta) -> Dict[str, Any]:
    return {
        "meta_version": "1.0",
        "step_key": spec.step_key,
        "name": spec.name,
        "script": spec.script,
        "status": meta.status,
        "elapsed_s": process_result.elapsed_s,
        "items": meta.items,
        "call_stats": meta.call_stats,
        "effective_timeout_s": process_result.effective_timeout_s,
        "cooldown_s": process_result.cooldown_s,
        "output_path": str(spec.output_path) if spec.output_path else None,
        "failed_items": meta.failed_items,
        "details": meta.details,
    }


def build_step_result(spec: StepSpec, process_result: ProcessResult, meta: StepMeta) -> Dict[str, Any]:
    return {
        "step_key": spec.step_key,
        "name": spec.name,
        "status": meta.status,
        "elapsed_s": process_result.elapsed_s,
        "items": meta.items,
        "effective_timeout_s": process_result.effective_timeout_s,
        "cooldown_s": process_result.cooldown_s,
        "stderr_tail": process_result.stderr_tail,
    }


def make_process_result(
    *,
    spec: StepSpec,
    status: str,
    timeout: int,
    elapsed_s: float = 0.0,
    stderr_tail: Optional[Sequence[str]] = None,
    stdout_tail: Optional[Sequence[str]] = None,
) -> ProcessResult:
    return ProcessResult(
        step_key=spec.step_key,
        name=spec.name,
        status=status,
        elapsed_s=round(elapsed_s, 1),
        effective_timeout_s=timeout,
        cooldown_s=spec.cooldown_s,
        stderr_tail=[str(line).strip() for line in (stderr_tail or []) if str(line).strip()],
        stdout_tail=[str(line).strip() for line in (stdout_tail or []) if str(line).strip()],
    )


def resolve_script_path(script: str) -> Path:
    path = Path(script)
    if path.is_absolute():
        return path
    return SCRIPTS_DIR / script


def run_step_process(
    spec: StepSpec,
    *,
    timeout: int,
    force: bool = False,
    output_flag: Optional[str] = "--output",
    respect_interrupt: bool = True,
) -> ProcessResult:
    if respect_interrupt and INTERRUPT_EVENT.is_set():
        return make_process_result(
            spec=spec,
            status="timeout",
            timeout=timeout,
            stderr_tail=[f"Interrupted before step start ({INTERRUPT_REASON})"],
        )

    cmd = [sys.executable, str(resolve_script_path(spec.script)), *spec.args]
    if spec.output_path is not None and output_flag:
        cmd += [output_flag, str(spec.output_path)]
    if force:
        cmd.append("--force")

    t0 = time.time()
    try:
        process = subprocess.Popen(
            cmd,
            text=True,
            env=os.environ,
            start_new_session=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = t0 + timeout
        stdout = ""
        stderr = ""
        while True:
            remaining = max(deadline - time.time(), 0.0)
            wait_slice = min(0.5, remaining) if remaining > 0 else 0.0
            try:
                stdout, stderr = process.communicate(timeout=wait_slice)
                break
            except subprocess.TimeoutExpired:
                if respect_interrupt and INTERRUPT_EVENT.is_set():
                    if os.name != "nt":
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                    else:
                        process.kill()
                    try:
                        stdout, stderr = process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        if os.name != "nt":
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                        else:
                            process.kill()
                        stdout, stderr = process.communicate()
                    return make_process_result(
                        spec=spec,
                        status="timeout",
                        timeout=timeout,
                        elapsed_s=time.time() - t0,
                        stderr_tail=[f"Interrupted ({INTERRUPT_REASON})"],
                        stdout_tail=(stdout or "").splitlines()[-3:],
                    )
                if time.time() >= deadline:
                    if os.name != "nt":
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                        try:
                            stdout, stderr = process.communicate(timeout=5)
                        except subprocess.TimeoutExpired:
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            stdout, stderr = process.communicate()
                    else:
                        process.kill()
                        stdout, stderr = process.communicate()
                    return make_process_result(
                        spec=spec,
                        status="timeout",
                        timeout=timeout,
                        elapsed_s=time.time() - t0,
                        stderr_tail=[f"Killed after {timeout}s"],
                        stdout_tail=(stdout or "").splitlines()[-3:],
                    )

        status = "ok" if process.returncode == 0 else "error"
        return make_process_result(
            spec=spec,
            status=status,
            timeout=timeout,
            elapsed_s=time.time() - t0,
            stderr_tail=(stderr or "").splitlines()[-3:] if status != "ok" else [],
            stdout_tail=(stdout or "").splitlines()[-3:],
        )
    except Exception as exc:
        return make_process_result(
            spec=spec,
            status="error",
            timeout=timeout,
            elapsed_s=time.time() - t0,
            stderr_tail=[str(exc)],
        )


def load_step_payload(output_path: Optional[Path]) -> Optional[Dict[str, Any]]:
    return load_json_file(output_path)


def write_step_meta(debug_dir: Path, spec: StepSpec, process_result: ProcessResult, meta: StepMeta) -> Path:
    step_meta_path = debug_dir / f"{spec.step_key}.meta.json"
    write_json(step_meta_path, serialize_step_meta(spec, process_result, meta))
    return step_meta_path


def finalize_step(debug_dir: Path, spec: StepSpec, process_result: ProcessResult) -> Tuple[Dict[str, Any], Path]:
    payload = load_step_payload(spec.output_path)
    meta = build_diagnostics(payload, process_result, spec.step_key)
    step_meta_path = write_step_meta(debug_dir, spec, process_result, meta)
    return build_step_result(spec, process_result, meta), step_meta_path


def build_fetch_steps(
    *,
    defaults_dir: Path,
    config_dir: Optional[Path],
    hours: int,
    debug_dir: Path,
    verbose: bool,
) -> List[StepSpec]:
    common = ["--defaults", str(defaults_dir)]
    if config_dir:
        common += ["--config", str(config_dir)]
    common += ["--hours", str(hours)]
    verbose_flag = ["--verbose"] if verbose else []

    return [
        StepSpec("rss", "RSS", "fetch-rss.py", common + verbose_flag, debug_dir / "rss.json", None),
        StepSpec("twitter", "Twitter", "fetch-twitter.py", common + verbose_flag, debug_dir / "twitter.json", get_cooldown_for_script("fetch-twitter.py")),
        StepSpec("google", "Google News", "fetch-google.py", common + verbose_flag, debug_dir / "google.json", get_cooldown_for_script("fetch-google.py")),
        StepSpec("github", "GitHub", "fetch-github.py", common + verbose_flag, debug_dir / "github.json", get_cooldown_for_script("fetch-github.py")),
        StepSpec(
            "trending",
            "GitHub Trending",
            "fetch-github-trending.py",
            ["--hours", str(hours), "--defaults", str(defaults_dir)] + (["--config", str(config_dir)] if config_dir else []) + verbose_flag,
            debug_dir / "trending.json",
            get_cooldown_for_script("fetch-github-trending.py"),
        ),
        StepSpec("api", "API Sources", "fetch-api.py", verbose_flag, debug_dir / "api.json", None),
        StepSpec("v2ex", "V2EX Hot", "fetch-v2ex.py", verbose_flag, debug_dir / "v2ex.json", get_cooldown_for_script("fetch-v2ex.py")),
        StepSpec("reddit", "Reddit", "fetch-reddit.py", common + verbose_flag, debug_dir / "reddit.json", get_cooldown_for_script("fetch-reddit.py")),
    ]


def log_step_completion(logger: logging.Logger, result: Dict[str, Any]) -> None:
    status_icon = {"ok": "✅", "error": "❌", "timeout": "⏰", "skipped": "⏭️", "pending": "…"}.get(result["status"], "?")
    logger.info("  %s %s: %s items (%ss)", status_icon, result["name"], result["items"], result["elapsed_s"])
    if result["status"] not in {"ok", "skipped"} and result["stderr_tail"]:
        for line in result["stderr_tail"]:
            logger.debug("    %s", line)


def execute_fetch_steps(
    *,
    steps: List[StepSpec],
    skip_steps: set[str],
    timeout: int,
    force: bool,
    debug_dir: Path,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Dict[str, str], float]:
    t_start = time.time()
    step_results: List[Dict[str, Any]] = []
    step_meta_paths: Dict[str, str] = {}
    active_specs: List[StepSpec] = []

    for spec in steps:
        if spec.step_key in skip_steps:
            skipped_result = make_process_result(spec=spec, status="skipped", timeout=timeout)
            result, meta_path = finalize_step(debug_dir, spec, skipped_result)
            step_results.append(result)
            step_meta_paths[spec.step_key] = str(meta_path)
            logger.info("  ⏭️  %s: skipped (--skip)", spec.name)
            continue

        pending_result = make_process_result(spec=spec, status="pending", timeout=timeout)
        pending_meta = build_diagnostics(None, pending_result, spec.step_key)
        meta_path = write_step_meta(debug_dir, spec, pending_result, pending_meta)
        step_meta_paths[spec.step_key] = str(meta_path)
        active_specs.append(spec)

    if active_specs:
        spec_by_future: Dict[Any, StepSpec] = {}
        with ThreadPoolExecutor(max_workers=len(active_specs)) as pool:
            for spec in active_specs:
                future = pool.submit(run_step_process, spec, timeout=timeout, force=force)
                spec_by_future[future] = spec

            for future in as_completed(spec_by_future):
                spec = spec_by_future[future]
                try:
                    process_result = future.result()
                except Exception as exc:
                    logger.exception("❌ %s future crashed", spec.name)
                    process_result = make_process_result(
                        spec=spec,
                        status="error",
                        timeout=timeout,
                        stderr_tail=[f"future crashed: {exc}"],
                    )
                result, meta_path = finalize_step(debug_dir, spec, process_result)
                step_results.append(result)
                step_meta_paths[spec.step_key] = str(meta_path)
                log_step_completion(logger, result)

    completed = {result["step_key"] for result in step_results}
    for spec in steps:
        if spec.step_key in completed:
            continue
        fallback = make_process_result(
            spec=spec,
            status="error",
            timeout=timeout,
            stderr_tail=["step finished without final result"],
        )
        result, meta_path = finalize_step(debug_dir, spec, fallback)
        step_results.append(result)
        step_meta_paths[spec.step_key] = str(meta_path)

    return step_results, step_meta_paths, time.time() - t_start


def build_merge_args(debug_dir: Path, archive_dir: Optional[Path], verbose: bool) -> List[str]:
    merge_args = ["--verbose"] if verbose else []
    for flag, path in [
        ("--rss", debug_dir / "rss.json"),
        ("--twitter", debug_dir / "twitter.json"),
        ("--google", debug_dir / "google.json"),
        ("--github", debug_dir / "github.json"),
        ("--trending", debug_dir / "trending.json"),
        ("--api", debug_dir / "api.json"),
        ("--v2ex", debug_dir / "v2ex.json"),
        ("--reddit", debug_dir / "reddit.json"),
    ]:
        if path.exists():
            merge_args += [flag, str(path)]
    if archive_dir:
        merge_args += ["--archive", str(archive_dir)]
    return merge_args


def run_merge_step(
    debug_dir: Path,
    archive_dir: Optional[Path],
    verbose: bool,
    *,
    respect_interrupt: bool = True,
) -> Tuple[StepSpec, Dict[str, Any], Path]:
    spec = StepSpec("merge", "Merge", "merge-sources.py", build_merge_args(debug_dir, archive_dir, verbose), debug_dir / "merged.json", None)
    process_result = run_step_process(spec, timeout=MERGE_TIMEOUT, force=False, respect_interrupt=respect_interrupt)
    result, meta_path = finalize_step(debug_dir, spec, process_result)
    return spec, result, meta_path


def run_hotspots_step(
    debug_dir: Path,
    archive_dir: Path,
    hotspots_top: int,
    mode: str,
    *,
    respect_interrupt: bool = True,
) -> Tuple[StepSpec, Dict[str, Any], Path, Dict[str, str]]:
    debug_output = debug_dir / "merge-hotspots.json"
    spec = StepSpec(
        "merge-hotspots",
        "Hotspots",
        "merge-hotspots.py",
        ["--input", str(debug_dir / "merged.json"), "--archive", str(archive_dir), "--debug", str(debug_dir), "--top", str(hotspots_top), "--mode", str(mode)],
        debug_output,
        None,
    )
    if not (debug_dir / "merged.json").exists():
        process_result = make_process_result(spec=spec, status="skipped", timeout=SUMMARY_TIMEOUT)
    else:
        process_result = run_step_process(spec, timeout=SUMMARY_TIMEOUT, force=False, output_flag=None, respect_interrupt=respect_interrupt)
    result, meta_path = finalize_step(debug_dir, spec, process_result)
    return spec, result, meta_path, parse_step_output_paths(process_result.stdout_tail)


def recover_partial_outputs(
    *,
    debug_dir: Path,
    archive_dir: Optional[Path],
    verbose: bool,
    hotspots_top: int,
    mode: str,
    logger: logging.Logger,
) -> Tuple[Dict[str, Any], Path, Dict[str, Any], Path, Dict[str, str]]:
    logger.info("🛟 Attempting partial recovery from completed step outputs...")
    _, merge_result, merge_meta_path = run_merge_step(debug_dir, archive_dir, verbose, respect_interrupt=False)
    if merge_result["status"] == "ok" and archive_dir:
        _, hotspots_result, hotspots_meta_path, hotspots_outputs = run_hotspots_step(
            debug_dir,
            archive_dir,
            hotspots_top,
            mode,
            respect_interrupt=False,
        )
    else:
        hotspots_outputs = {}
        hotspots_spec = StepSpec("merge-hotspots", "Hotspots", "merge-hotspots.py", [], debug_dir / "merge-hotspots.json", None)
        skipped_hotspots = make_process_result(
            spec=hotspots_spec,
            status="skipped" if merge_result["status"] == "ok" else "timeout",
            timeout=SUMMARY_TIMEOUT,
            stderr_tail=[f"Skipped during recovery ({INTERRUPT_REASON})"] if merge_result["status"] != "ok" else [],
        )
        hotspots_result, hotspots_meta_path = finalize_step(debug_dir, hotspots_spec, skipped_hotspots)
    return merge_result, merge_meta_path, hotspots_result, hotspots_meta_path, hotspots_outputs


def build_pipeline_failed_items(step_results: List[Dict[str, Any]], extra_results: Sequence[Dict[str, Any]]) -> List[Dict[str, str]]:
    failed_items: List[Dict[str, str]] = []

    def append_item(item_id: str, error: str) -> None:
        text = normalize_error_text(error)
        if not text:
            return
        candidate = {"id": item_id, "error": text}
        if candidate not in failed_items:
            failed_items.append(candidate)

    for result in [*step_results, *extra_results]:
        if result.get("status") not in {"error", "timeout"}:
            continue
        if result.get("failed_items"):
            first = result["failed_items"][0]
            append_item(str(result.get("step_key", "unknown")), str(first.get("error", "")))
            continue
        stderr_tail = [str(line).strip() for line in result.get("stderr_tail", []) if str(line).strip()]
        append_item(str(result.get("step_key") or result.get("name") or "unknown"), stderr_tail[0] if stderr_tail else str(result.get("status", "error")))
    return failed_items[:20]


def write_pipeline_meta(
    *,
    debug_dir: Path,
    step_results: List[Dict[str, Any]],
    step_meta_paths: Dict[str, str],
    merge_result: Dict[str, Any],
    hotspots_result: Dict[str, Any],
    hotspots_output: Optional[Path],
    markdown_output: Optional[Path],
    hotspots_mode: str,
    fetch_elapsed: float,
    total_elapsed: float,
    interrupted: bool = False,
    interruption_reason: Optional[str] = None,
) -> Path:
    meta_output = debug_dir / "pipeline.meta.json"
    pipeline_items = merge_result.get("items", 0)
    ok_calls = (
        sum(1 for result in step_results if result.get("status") == "ok")
        + (1 if merge_result.get("status") == "ok" else 0)
        + (1 if hotspots_result.get("status") == "ok" else 0)
    )
    failed_calls = (
        sum(1 for result in step_results if result.get("status") in {"error", "timeout"})
        + (1 if merge_result.get("status") in {"error", "timeout"} else 0)
        + (1 if hotspots_result.get("status") in {"error", "timeout"} else 0)
    )

    meta = {
        "pipeline_version": "2.0.0",
        "debug_dir": str(debug_dir),
        "total_elapsed_s": round(total_elapsed, 1),
        "fetch_elapsed_s": round(fetch_elapsed, 1),
        "overall_status": (
            "timeout"
            if interrupted
            else ("error" if merge_result["status"] != "ok" or hotspots_result["status"] != "ok" else "ok")
        ),
        "steps": step_results,
        "step_meta_paths": step_meta_paths,
        "items": pipeline_items,
        "call_stats": {
            "kind": "steps",
            "total_calls": len(step_results) + 2,
            "ok_calls": ok_calls,
            "failed_calls": failed_calls,
        },
        "failed_items": build_pipeline_failed_items(step_results, [merge_result, hotspots_result]),
        "merge": merge_result,
        "hotspots_format": "json",
        "hotspots_mode": hotspots_mode,
        "hotspots_status": hotspots_result.get("status"),
        "hotspots_elapsed_s": hotspots_result.get("elapsed_s"),
        "hotspots_output": str(hotspots_output) if hotspots_output else None,
        "markdown_output": str(markdown_output) if markdown_output else None,
        "interrupted": interrupted,
        "interruption_reason": interruption_reason if interrupted else None,
    }
    write_json(meta_output, meta)
    return meta_output


def log_pipeline_summary(
    logger: logging.Logger,
    *,
    total_elapsed: float,
    step_results: List[Dict[str, Any]],
    merge_result: Dict[str, Any],
    hotspots_result: Dict[str, Any],
    hotspots_output: Optional[Path],
    markdown_output: Optional[Path],
    meta_output: Path,
    debug_dir: Path,
) -> None:
    logger.info("%s", "=" * 50)
    logger.info("📊 Pipeline Summary (%.1fs total)", total_elapsed)
    for result in [*step_results, merge_result, hotspots_result]:
        logger.info("   %-14s %-8s %4d items %6.1fs", result["name"], result["status"], result["items"], result["elapsed_s"])
    if hotspots_output:
        logger.info("   Hotspots: %s", hotspots_output)
    if markdown_output:
        logger.info("   Markdown: %s", markdown_output)
    logger.info("   Meta: %s", meta_output)
    logger.info("   Debug Dir: %s", debug_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full news-hotspots pipeline and produce a compact hotspots output.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Skill defaults config dir")
    parser.add_argument("--config", type=Path, default=Path("workspace/config"), help="User config overlay dir (default: workspace/config)")
    parser.add_argument("--hours", type=int, default=48, help="Time window in hours")
    parser.add_argument("--archive", dest="archive", type=Path, default=Path("workspace/archive/news-hotspots"), help="Archive root dir for final hotspots outputs (default: workspace/archive/news-hotspots)")
    parser.add_argument("--debug", type=Path, default=None, help="Directory for debug and intermediate files")
    parser.add_argument("--mode", type=str, default="daily", choices=["daily", "weekly"], help="Final hotspots mode label and archive file stem")
    parser.add_argument("--top", type=int, default=DEFAULT_HOTSPOTS_TOP, help="Top N items per topic in hotspots output")
    parser.add_argument("--step-timeout", type=int, default=DEFAULT_TIMEOUT, help="Per-step timeout in seconds (default: 1800)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--force", action="store_true", help="Force re-fetch ignoring caches")
    parser.add_argument("--skip", type=str, default="", help="Comma-separated list of steps to skip")
    return parser.parse_args()


def main() -> int:
    global INTERRUPT_REASON
    INTERRUPT_EVENT.clear()
    INTERRUPT_REASON = "external interrupt"
    args = parse_args()
    logger = setup_logging(args.verbose)
    previous_signal_handlers = install_signal_handlers(logger)
    skip_steps = {item.strip().lower() for item in args.skip.split(",") if item.strip()}
    config_dir = args.config if args.config and args.config.exists() else None
    debug_dir = resolve_debug_dir(args.debug)
    if args.archive:
        args.archive.mkdir(parents=True, exist_ok=True)

    logger.info("📁 Debug directory: %s", debug_dir)
    logger.info("📝 Final hotspots outputs will be archived under: %s", args.archive)
    logger.info("🗂️ Hotspots mode: %s", args.mode)
    if args.config and not args.config.exists():
        logger.info("ℹ️ Config overlay not found, using defaults only: %s", args.config)
    if args.archive:
        logger.info("🗄️ Archive root: %s", args.archive)

    steps = build_fetch_steps(
        defaults_dir=args.defaults,
        config_dir=config_dir,
        hours=args.hours,
        debug_dir=debug_dir,
        verbose=args.verbose,
    )

    try:
        logger.info("🚀 Starting pipeline: %d/%d sources, %sh window", len([step for step in steps if step.step_key not in skip_steps]), len(steps), args.hours)
        t_start = time.time()

        step_results, step_meta_paths, fetch_elapsed = execute_fetch_steps(
            steps=steps,
            skip_steps=skip_steps,
            timeout=args.step_timeout,
            force=args.force,
            debug_dir=debug_dir,
            logger=logger,
        )
        logger.info("📡 Fetch phase done in %.1fs", fetch_elapsed)

        if INTERRUPT_EVENT.is_set():
            logger.warning("⚠️ Pipeline interrupted during fetch phase (%s)", INTERRUPT_REASON)
            merge_result, merge_meta_path, hotspots_result, hotspots_meta_path, hotspots_outputs = recover_partial_outputs(
                debug_dir=debug_dir,
                archive_dir=args.archive,
                verbose=args.verbose,
                hotspots_top=args.top,
                mode=args.mode,
                logger=logger,
            )
        else:
            logger.info("🔀 Merging & scoring...")
            _, merge_result, merge_meta_path = run_merge_step(debug_dir, args.archive, args.verbose)
            if merge_result["status"] == "ok":
                logger.info("🧾 Rendering hotspots...")
                _, hotspots_result, hotspots_meta_path, hotspots_outputs = run_hotspots_step(debug_dir, args.archive, args.top, args.mode)
            else:
                hotspots_outputs = {}
                hotspots_spec = StepSpec("merge-hotspots", "Hotspots", "merge-hotspots.py", [], debug_dir / "merge-hotspots.json", None)
                skipped_hotspots = make_process_result(spec=hotspots_spec, status="skipped", timeout=SUMMARY_TIMEOUT)
                hotspots_result, hotspots_meta_path = finalize_step(debug_dir, hotspots_spec, skipped_hotspots)
            if INTERRUPT_EVENT.is_set() and (merge_result["status"] != "ok" or hotspots_result["status"] != "ok"):
                logger.warning("⚠️ Pipeline interrupted during merge/render phase (%s)", INTERRUPT_REASON)
                merge_result, merge_meta_path, hotspots_result, hotspots_meta_path, hotspots_outputs = recover_partial_outputs(
                    debug_dir=debug_dir,
                    archive_dir=args.archive,
                    verbose=args.verbose,
                    hotspots_top=args.top,
                    mode=args.mode,
                    logger=logger,
                )

        step_meta_paths["merge"] = str(merge_meta_path)
        step_meta_paths["hotspots"] = str(hotspots_meta_path)
        hotspots_output = Path(hotspots_outputs["ARCHIVED_JSON"]) if hotspots_outputs.get("ARCHIVED_JSON") else None
        markdown_output = Path(hotspots_outputs["ARCHIVED_MARKDOWN"]) if hotspots_outputs.get("ARCHIVED_MARKDOWN") else None

        total_elapsed = time.time() - t_start
        meta_output = write_pipeline_meta(
            debug_dir=debug_dir,
            step_results=step_results,
            step_meta_paths=step_meta_paths,
            merge_result=merge_result,
            hotspots_result=hotspots_result,
            hotspots_output=hotspots_output,
            markdown_output=markdown_output if markdown_output and markdown_output.exists() else None,
            hotspots_mode=args.mode,
            fetch_elapsed=fetch_elapsed,
            total_elapsed=total_elapsed,
            interrupted=INTERRUPT_EVENT.is_set(),
            interruption_reason=INTERRUPT_REASON,
        )

        removed_archive_dirs = cleanup_archive_root(args.archive) if args.archive else 0
        archived_outputs = archive_meta_outputs(args.archive, meta_output, step_meta_paths)
        if removed_archive_dirs:
            logger.info("🧹 Removed %d expired archive date directories", removed_archive_dirs)
        if hotspots_output:
            logger.info("🗂️  Archived hotspots JSON: %s", hotspots_output)
        if markdown_output:
            logger.info("🗂️  Archived hotspots Markdown: %s", markdown_output)
        if archived_outputs.get("pipeline_meta"):
            logger.info("🗂️  Archived meta dir: %s", archived_outputs.get("meta_dir"))

        meta = load_json_file(meta_output) or {}
        meta["archive"] = {
            "root": str(args.archive) if args.archive else None,
            "retention_days": ARCHIVE_RETENTION_DAYS,
            "removed_expired_date_dirs": removed_archive_dirs,
            "hotspots_json": str(hotspots_output) if hotspots_output else None,
            "hotspots_markdown": str(markdown_output) if markdown_output else None,
            **archived_outputs,
        }
        write_json(meta_output, meta)

        log_pipeline_summary(
            logger,
            total_elapsed=total_elapsed,
            step_results=step_results,
            merge_result=merge_result,
            hotspots_result=hotspots_result,
            hotspots_output=hotspots_output,
            markdown_output=markdown_output if markdown_output and markdown_output.exists() else None,
            meta_output=meta_output,
            debug_dir=debug_dir,
        )

        if INTERRUPT_EVENT.is_set():
            logger.warning("⚠️ Pipeline ended after interruption: %s", INTERRUPT_REASON)
            logger.warning("⚠️ Run source-health.py to report completed vs incomplete steps.")
            if markdown_output and markdown_output.exists():
                logger.info("📣 Partial Markdown available: %s", markdown_output)
            return 1
        if merge_result["status"] != "ok":
            logger.error("❌ Merge failed: %s", merge_result["stderr_tail"])
            return 1
        if hotspots_result["status"] != "ok":
            logger.error("❌ Hotspots failed: %s", hotspots_result["stderr_tail"])
            return 1

        final_markdown_path = str(markdown_output) if markdown_output else None
        logger.info("📣 Final Markdown: %s", final_markdown_path)
        logger.info("✅ Done → %s", final_markdown_path or args.archive)
        return 0
    finally:
        restore_signal_handlers(previous_signal_handlers)


if __name__ == "__main__":
    raise SystemExit(main())
