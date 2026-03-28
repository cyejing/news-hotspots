#!/usr/bin/env python3
"""
Step health diagnostics for news-digest pipeline.

Reads per-step metadata JSON files and prints current and recent historical
diagnostics directly from those metadata files.
"""

import argparse
import logging
import re
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

HISTORY_DAYS = 7
DEGRADED_THRESHOLD = 0.5
REPORT_LIMIT = 20
META_FILE_RE = re.compile(r".*\.meta\d*\.json$")
META_SUFFIX_RE = re.compile(r"\.meta(\d*)\.json$")
DEFAULT_INPUT_DIR = Path("/tmp/news-digest/debug")


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
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date()
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
            date_value = datetime.strptime(parent.name, "%Y-%m-%d").replace(tzinfo=timezone.utc)
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
    suffix = match.group(1)
    run_index = 1 if suffix == "" else int(suffix) + 1
    return f"{date_label}-{run_index}"


def build_direct_run_label(input_dir: Path, now_ts: float) -> str:
    date_label = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return f"{date_label}-current"


def compute_pipeline_state(meta: Dict[str, Any]) -> Dict[str, Any]:
    steps = [step for step in meta.get("steps", []) if isinstance(step, dict)]
    failed_steps = [step.get("name", "unknown") for step in steps if step.get("status") in {"error", "timeout"}]
    skipped_steps = [step.get("name", "unknown") for step in steps if step.get("status") == "skipped"]
    merge_status = meta.get("merge", {}).get("status", "error") if isinstance(meta.get("merge"), dict) else "error"
    summary_status = meta.get("summary_status", "error")
    overall_status = meta.get("overall_status", "error")

    if overall_status in {"error", "timeout"} or merge_status != "ok" or summary_status != "ok":
        state = "error"
    elif failed_steps:
        state = "error"
    elif skipped_steps:
        state = "warn"
    else:
        state = "ok"

    failed_items = meta.get("failed_items", [])

    return {
        "step_key": "pipeline",
        "name": "Pipeline",
        "status": overall_status,
        "state": state,
        "elapsed_s": float(meta.get("total_elapsed_s", 0) or 0),
        "count": int(meta.get("merge", {}).get("count", 0) or 0) if isinstance(meta.get("merge"), dict) else 0,
        "failed_records": len(failed_items) or len(failed_steps),
        "failed_items": failed_items if isinstance(failed_items, list) else [],
        "details": {
            "pipeline": {
                "fetch_elapsed_s": meta.get("fetch_elapsed_s", 0),
                "failed_steps": failed_steps,
                "skipped_steps": skipped_steps,
                "summary_status": summary_status,
                "step_count": len(steps),
            }
        },
        "reasons": [],
    }


def compute_step_state(meta: Dict[str, Any]) -> Dict[str, Any]:
    if "pipeline_version" in meta:
        return compute_pipeline_state(meta)

    status = meta.get("status", "error")
    details = meta.get("details", {}) if isinstance(meta.get("details"), dict) else {}
    record_summary = details.get("record_summary", {}) if isinstance(details.get("record_summary"), dict) else {}
    failed_records = int(record_summary.get("error", 0) or 0)
    warning_reasons: List[str] = []

    if status in {"error", "timeout"}:
        state = "error"
    elif status == "skipped":
        state = "skipped"
    else:
        state = "ok"

    if failed_records > 0 and state == "ok":
        state = "warn"
        warning_reasons.append(f"{failed_records} record failures")

    deduplication = details.get("deduplication", {}) if isinstance(details.get("deduplication"), dict) else {}
    if deduplication:
        dropped = deduplication.get("dropped", 0)
        drop_ratio = deduplication.get("drop_ratio", 0.0)
        warning_reasons.append(f"dedup {dropped} dropped ({drop_ratio:.0%})")

    processing = details.get("processing", {}) if isinstance(details.get("processing"), dict) else {}
    if processing.get("scoring_version"):
        warning_reasons.append(f"scoring v{processing['scoring_version']}")

    return {
        "step_key": meta.get("step_key", "unknown"),
        "name": meta.get("name", meta.get("step_key", "unknown")),
        "status": status,
        "state": state,
        "elapsed_s": float(meta.get("elapsed_s", 0) or 0),
        "count": int(meta.get("count", 0) or 0),
        "failed_records": failed_records,
        "failed_items": meta.get("failed_items", []),
        "details": details,
        "reasons": warning_reasons,
    }


def build_history_rows(diagnostics: List[Dict[str, Any]], now: float) -> List[Dict[str, Any]]:
    cutoff = now - HISTORY_DAYS * 86400
    grouped: Dict[str, Dict[str, Any]] = {}
    for diagnostic in diagnostics:
        observed_ts = float(diagnostic.get("observed_ts", now))
        if observed_ts <= cutoff:
            continue
        step_key = diagnostic["step_key"]
        if step_key not in grouped:
            grouped[step_key] = {"name": diagnostic["name"], "checks": []}
        grouped[step_key]["name"] = diagnostic["name"]
        grouped[step_key]["checks"].append(
            {
                "ts": observed_ts,
                "state": diagnostic["state"],
                "status": diagnostic["status"],
                "elapsed_s": diagnostic["elapsed_s"],
                "count": diagnostic["count"],
                "failed_records": diagnostic["failed_records"],
                "error_summary": str(diagnostic["failed_items"][0]["error"]).strip() if diagnostic.get("failed_items") else "",
                "failed_items": [
                    {
                        "id": str(item.get("id", "unknown")).strip() or "unknown",
                        "error": str(item.get("error", "unknown error")).strip() or "unknown error",
                    }
                    for item in diagnostic.get("failed_items", [])
                    if isinstance(item, dict)
                ][:10],
            }
        )

    rows: List[Dict[str, Any]] = []
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
        rows.append(
            {
                "step_key": step_key,
                "name": info.get("name", step_key),
                "checks": len(checks),
                "ok": ok_count,
                "warn": warn_count,
                "error": error_count,
                "degraded_rate": degraded_rate,
                "unhealthy": len(checks) >= 2 and degraded_rate > DEGRADED_THRESHOLD,
                "median_elapsed_s": statistics.median([check["elapsed_s"] for check in checks]) if checks else 0.0,
                "latest_issue_ts": latest_issue["ts"] if latest_issue else None,
                "latest_issue_summary": latest_issue.get("error_summary", "") if latest_issue else "",
                "check_details": checks_sorted,
            }
        )
    rows.sort(key=lambda row: (-row["degraded_rate"], -row["error"], row["name"]))
    return rows


def print_history_report(history_rows: List[Dict[str, Any]], logger: logging.Logger) -> int:
    unhealthy = [row for row in history_rows if row["unhealthy"]]
    logger.info(
        f"History report: {len(history_rows)} steps tracked, {len(unhealthy)} unhealthy in last {HISTORY_DAYS} days"
    )
    for row in history_rows[:REPORT_LIMIT]:
        if row["unhealthy"]:
            icon = "⚠️"
        elif row["warn"] > 0 or row["error"] > 0:
            icon = "🟡"
        else:
            icon = "✅"
        logger.info(
            f"{icon} {row['name']} - ok:{row['ok']} warn:{row['warn']} error:{row['error']} "
            f"({row['degraded_rate']:.0%} degraded)"
        )
    return len(unhealthy)


def print_run_details(diagnostics: List[Dict[str, Any]], logger: logging.Logger) -> None:
    run_groups: Dict[str, List[Dict[str, Any]]] = {}
    for diagnostic in diagnostics:
        run_label = diagnostic.get("run_label")
        if not run_label:
            continue
        run_groups.setdefault(run_label, []).append(diagnostic)

    if not run_groups:
        return

    def sort_key(label: str) -> tuple[str, int]:
        date_part, _, run_part = label.rpartition("-")
        try:
            return (date_part, int(run_part))
        except ValueError:
            return (label, 0)

    logger.info("Run details:")
    for run_label in sorted(run_groups.keys(), key=sort_key, reverse=True):
        title = f"=== {run_label} ==="
        border = "=" * len(title)
        logger.info("")
        logger.info(border)
        logger.info(title)
        logger.info(border)
        run_items = sorted(
            run_groups[run_label],
            key=lambda item: (0 if item["step_key"] == "pipeline" else 1, item["name"]),
        )
        for item in run_items:
            if item["state"] == "error":
                icon = "⚠️"
            elif item["state"] == "warn":
                icon = "🟡"
            elif item["state"] == "skipped":
                icon = "⏭️"
            else:
                icon = "✅"

            logger.info(
                "%s %s   count:%s | failed:%s | elapsed:%.1fs",
                icon,
                item["name"],
                item.get("count", 0),
                item.get("failed_records", 0),
                float(item.get("elapsed_s", 0) or 0),
            )
            for failed_item in item.get("failed_items", []):
                logger.info("   - %s: %s", failed_item.get("id", "unknown"), failed_item.get("error", "unknown error"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read step metadata and report pipeline health diagnostics.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory to inspect. Reads direct *.meta*.json files in this directory and recent <DATE>/meta/*.meta*.json below it.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    now = time.time()
    current_diagnostics: List[Dict[str, Any]] = []
    meta_files = discover_all_meta_files(args.input_dir, HISTORY_DAYS)
    direct_run_label = build_direct_run_label(args.input_dir, now)
    for path in meta_files:
        payload = load_json(path)
        if not payload:
            logger.debug(f"skip invalid meta file: {path}")
            continue
        diagnostic = compute_step_state(payload)
        if "meta" in {parent.name for parent in path.parents}:
            diagnostic["observed_ts"] = parse_archive_observed_ts(path)
            diagnostic["run_label"] = parse_archive_run_label(path)
        else:
            diagnostic["run_label"] = direct_run_label
        current_diagnostics.append(diagnostic)

    logger.info(f"Loaded {len(current_diagnostics)} metadata files")
    history_rows = build_history_rows(current_diagnostics, now)
    unhealthy = print_history_report(history_rows, logger)
    print_run_details([item for item in current_diagnostics if item.get("run_label")], logger)
    return 0 if unhealthy >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
