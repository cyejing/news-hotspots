#!/usr/bin/env python3
"""
Unified data collection pipeline for news-digest.

Runs fetch steps, merges them into an internal JSON inside a debug directory,
then renders a compact summary JSON for downstream prompt-writing flows.
"""

import argparse
import shutil
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone

SCRIPTS_DIR = Path(__file__).parent
DEFAULT_TIMEOUT = 2000
MERGE_TIMEOUT = 300
SUMMARY_TIMEOUT = 120
DEFAULT_SUMMARY_TOP = 5
ARCHIVE_RETENTION_DAYS = 90
STEP_COOLDOWN_DEFAULTS = {
    "fetch-twitter.py": ("BB_BROWSER_TWITTER_COOLDOWN_SECONDS", 8.0),
    "fetch-reddit.py": ("BB_BROWSER_REDDIT_COOLDOWN_SECONDS", 6.0),
    "fetch-google.py": ("BB_BROWSER_GOOGLE_COOLDOWN_SECONDS", 8.0),
    "fetch-v2ex.py": ("BB_BROWSER_V2EX_COOLDOWN_SECONDS", 5.0),
    "fetch-github.py": ("NEWS_DIGEST_GITHUB_COOLDOWN_SECONDS", 2.0),
    "fetch-github-trending.py": ("NEWS_DIGEST_GITHUB_TRENDING_COOLDOWN_SECONDS", 2.0),
}


def load_json_file(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(__name__)


def count_output_items(output_path: Path) -> int:
    if not output_path.exists() or output_path.suffix != ".json":
        return 0
    try:
        with open(output_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return 0
    return (
        data.get("total_articles")
        or data.get("total_posts")
        or data.get("total_releases")
        or data.get("total_results")
        or data.get("total")
        or data.get("output_stats", {}).get("total_articles")
        or 0
    )


def run_step(
    name: str,
    script: str,
    args_list: list,
    output_path: Optional[Path],
    timeout: int = DEFAULT_TIMEOUT,
    force: bool = False,
    cooldown_s: Optional[float] = None,
    output_flag: str = "--output",
) -> Dict[str, Any]:
    t0 = time.time()
    cmd = [sys.executable, str(SCRIPTS_DIR / script)] + args_list
    if output_path is not None:
        cmd += [output_flag, str(output_path)]
    if force:
        cmd.append("--force")

    try:
        process = subprocess.Popen(
            cmd,
            text=True,
            env=os.environ,
            start_new_session=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
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
            elapsed = time.time() - t0
            return {
                "name": name,
                "status": "timeout",
                "elapsed_s": round(elapsed, 1),
                "count": 0,
                "effective_timeout_s": timeout,
                "cooldown_s": cooldown_s,
                "stderr_tail": [f"Killed after {timeout}s"],
            }

        elapsed = time.time() - t0
        ok = process.returncode == 0
        count = count_output_items(output_path) if ok and output_path is not None else 0
        return {
            "name": name,
            "status": "ok" if ok else "error",
            "elapsed_s": round(elapsed, 1),
            "count": count,
            "effective_timeout_s": timeout,
            "cooldown_s": cooldown_s,
            "stderr_tail": (stderr or "").strip().split("\n")[-3:] if not ok else [],
        }
    except Exception as exc:
        elapsed = time.time() - t0
        return {
            "name": name,
            "status": "error",
            "elapsed_s": round(elapsed, 1),
            "count": 0,
            "effective_timeout_s": timeout,
            "cooldown_s": cooldown_s,
            "stderr_tail": [str(exc)],
        }


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
    return Path(tempfile.mkdtemp(prefix="news-digest-pipeline-"))


def resolve_unique_output_path(path: Path) -> Path:
    if path == Path("/tmp/summary.json"):
        return path
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


def archive_run_artifacts(
    archive_root: Optional[Path],
    summary_output: Path,
    pipeline_meta_output: Path,
    step_meta_paths: Dict[str, str],
) -> Dict[str, Any]:
    if not archive_root:
        return {}

    today_dir = archive_root / datetime.now(timezone.utc).date().isoformat()
    json_dir = today_dir / "json"
    meta_dir = today_dir / "meta"
    json_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    archived: Dict[str, Any] = {"date_dir": str(today_dir), "json_dir": str(json_dir), "meta_dir": str(meta_dir)}

    if summary_output.exists():
        archived_summary = resolve_unique_output_path(json_dir / summary_output.name)
        shutil.copy2(summary_output, archived_summary)
        archived["summary_json"] = str(archived_summary)

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


def summarize_output_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
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
        details["summary_stats"] = {
            "total_articles": payload.get("total_articles", 0),
            "topic_count": len(payload["topic_order"]),
            "topic_order": payload["topic_order"],
            "source_breakdown": payload.get("source_breakdown", {}),
        }

    return details


def collect_failed_items(payload: Optional[Dict[str, Any]], limit: int = 10) -> List[Dict[str, str]]:
    if not payload:
        return []

    failed_items: List[Dict[str, str]] = []

    def append_item(entry_id: Any, error_value: Any) -> None:
        item_id = str(entry_id).strip()
        error_text = str(error_value).strip()
        if not item_id:
            item_id = "unknown"
        if not error_text:
            error_text = "unknown error"
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
            has_error = bool(explicit_error) or (isinstance(error_messages, list) and any(str(item).strip() for item in error_messages))
            if status == "ok" or (status is None and not has_error):
                continue
            entry_id = (
                entry.get("source_id")
                or entry.get("id")
                or entry.get("subreddit")
                or entry.get("topic")
                or entry.get("repo")
                or entry.get("name")
                or "unknown"
            )
            if explicit_error:
                append_item(entry_id, explicit_error)
            elif isinstance(error_messages, list) and error_messages:
                append_item(entry_id, error_messages[0])
            else:
                append_item(entry_id, "unknown error")
            if len(failed_items) >= limit:
                return failed_items[:limit]

    return failed_items[:limit]


def build_aggregate_failed_items(result: Dict[str, Any], limit: int = 1) -> List[Dict[str, str]]:
    if result.get("status") not in {"error", "timeout"}:
        return []
    stderr_tail = [str(line).strip() for line in result.get("stderr_tail", []) if str(line).strip()]
    message = stderr_tail[0] if stderr_tail else result.get("status", "error")
    return [{"id": "__step__", "error": str(message)}][:limit]


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def build_step_meta(
    *,
    step_key: str,
    name: str,
    script: str,
    result: Dict[str, Any],
    output_path: Optional[Path],
) -> Dict[str, Any]:
    payload = load_json_file(output_path)
    failed_items = collect_failed_items(payload)
    if not failed_items:
        failed_items = build_aggregate_failed_items(result)
    return {
        "meta_version": "1.0",
        "step_key": step_key,
        "name": name,
        "script": script,
        "status": result.get("status"),
        "elapsed_s": result.get("elapsed_s"),
        "count": result.get("count", 0),
        "effective_timeout_s": result.get("effective_timeout_s"),
        "cooldown_s": result.get("cooldown_s"),
        "output_path": str(output_path) if output_path else None,
        "failed_items": failed_items,
        "details": summarize_output_payload(payload),
    }


def build_pipeline_failed_items(
    step_results: List[Dict[str, Any]],
    merge_result: Dict[str, Any],
    summary_result: Dict[str, Any],
    limit: int = 20,
) -> List[Dict[str, str]]:
    failed_items: List[Dict[str, str]] = []

    def append_item(item_id: str, error: str) -> None:
        candidate = {"id": item_id, "error": error}
        if candidate not in failed_items:
            failed_items.append(candidate)

    for result in step_results:
        if result.get("status") not in {"error", "timeout"}:
            continue
        stderr_tail = [str(line).strip() for line in result.get("stderr_tail", []) if str(line).strip()]
        append_item(str(result.get("step_key") or result.get("name") or "unknown"), stderr_tail[0] if stderr_tail else str(result.get("status", "error")))

    for result, item_id in ((merge_result, "merge"), (summary_result, "summarize")):
        if result.get("status") not in {"error", "timeout"}:
            continue
        stderr_tail = [str(line).strip() for line in result.get("stderr_tail", []) if str(line).strip()]
        append_item(item_id, stderr_tail[0] if stderr_tail else str(result.get("status", "error")))

    return failed_items[:limit]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the full news-digest pipeline and produce a compact summary output.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Skill defaults config dir")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("workspace/config"),
        help="User config overlay dir (default: workspace/config)",
    )
    parser.add_argument("--hours", type=int, default=48, help="Time window in hours")
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("workspace/archive/news-digest"),
        help="Archive root dir for previous summary JSON files (default: workspace/archive/news-digest)",
    )
    parser.add_argument("--output", "-o", type=Path, required=True, help="Required output path for summary.json")
    parser.add_argument("--debug-dir", type=Path, default=None, help="Directory for debug and intermediate files")
    parser.add_argument("--summary-top", type=int, default=DEFAULT_SUMMARY_TOP, help="Top N items per topic in summary output")
    parser.add_argument(
        "--step-timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Per-step timeout in seconds (default: 1800)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--force", action="store_true", help="Force re-fetch ignoring caches")
    parser.add_argument("--skip", type=str, default="", help="Comma-separated list of steps to skip")

    args = parser.parse_args()
    logger = setup_logging(args.verbose)
    skip_steps = {item.strip().lower() for item in args.skip.split(",") if item.strip()}
    config_dir = args.config if args.config and args.config.exists() else None

    debug_dir = resolve_debug_dir(args.debug_dir)
    if args.archive_dir:
        args.archive_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    summary_output = resolve_unique_output_path(args.output)
    merged_output = debug_dir / "merged.json"
    meta_output = debug_dir / "pipeline.meta.json"

    tmp_rss = debug_dir / "rss.json"
    tmp_twitter = debug_dir / "twitter.json"
    tmp_google = debug_dir / "google.json"
    tmp_github = debug_dir / "github.json"
    tmp_trending = debug_dir / "trending.json"
    tmp_api = debug_dir / "api.json"
    tmp_v2ex = debug_dir / "v2ex.json"
    tmp_reddit = debug_dir / "reddit.json"

    logger.info("📁 Debug directory: %s", debug_dir)
    logger.info("📝 Summary JSON output: %s", summary_output)
    if args.config and not args.config.exists():
        logger.info("ℹ️ Config overlay not found, using defaults only: %s", args.config)
    if args.archive_dir:
        logger.info("🗄️ Archive root: %s", args.archive_dir)

    common = ["--defaults", str(args.defaults)]
    if config_dir:
        common += ["--config", str(config_dir)]
    common += ["--hours", str(args.hours)]
    verbose_flag = ["--verbose"] if args.verbose else []

    steps = [
        ("rss", "RSS", "fetch-rss.py", common + verbose_flag, tmp_rss, None),
        ("twitter", "Twitter", "fetch-twitter.py", common + verbose_flag, tmp_twitter, get_cooldown_for_script("fetch-twitter.py")),
        ("google", "Google News", "fetch-google.py", common + verbose_flag, tmp_google, get_cooldown_for_script("fetch-google.py")),
        ("github", "GitHub", "fetch-github.py", common + verbose_flag, tmp_github, get_cooldown_for_script("fetch-github.py")),
        (
            "trending",
            "GitHub Trending",
            "fetch-github-trending.py",
            ["--hours", str(args.hours), "--defaults", str(args.defaults)]
            + (["--config", str(config_dir)] if config_dir else [])
            + verbose_flag,
            tmp_trending,
            get_cooldown_for_script("fetch-github-trending.py"),
        ),
        ("api", "API Sources", "fetch-api.py", verbose_flag, tmp_api, None),
        ("v2ex", "V2EX Hot", "fetch-v2ex.py", verbose_flag, tmp_v2ex, get_cooldown_for_script("fetch-v2ex.py")),
        ("reddit", "Reddit", "fetch-reddit.py", common + verbose_flag, tmp_reddit, get_cooldown_for_script("fetch-reddit.py")),
    ]

    active_steps = []
    step_meta_paths: Dict[str, str] = {}
    for step_key, name, script, step_args, out_path, cooldown_s in steps:
        if step_key in skip_steps:
            logger.info("  ⏭️  %s: skipped (--skip)", name)
            skipped_result = {
                "name": name,
                "step_key": step_key,
                "status": "skipped",
                "elapsed_s": 0,
                "count": 0,
                "effective_timeout_s": args.step_timeout,
                "cooldown_s": cooldown_s,
                "stderr_tail": [],
            }
            step_meta_path = debug_dir / f"{step_key}.meta.json"
            write_json(
                step_meta_path,
                build_step_meta(
                    step_key=step_key,
                    name=name,
                    script=script,
                    result=skipped_result,
                    output_path=out_path,
                ),
            )
            step_meta_paths[step_key] = str(step_meta_path)
            continue
        active_steps.append((step_key, name, script, step_args, out_path, cooldown_s))

    logger.info("🚀 Starting pipeline: %d/%d sources, %sh window", len(active_steps), len(steps), args.hours)
    t_start = time.time()

    step_results = []
    if active_steps:
        with ThreadPoolExecutor(max_workers=len(active_steps)) as pool:
            futures = {}
            for step_key, name, script, step_args, out_path, cooldown_s in active_steps:
                future = pool.submit(
                    run_step,
                    name,
                    script,
                    step_args,
                    out_path,
                    args.step_timeout,
                    args.force,
                    cooldown_s,
                )
                futures[future] = (step_key, name, script, out_path)

            for future in as_completed(futures):
                result = future.result()
                step_key, name, script, out_path = futures[future]
                result["step_key"] = step_key
                step_results.append(result)
                step_meta_path = debug_dir / f"{step_key}.meta.json"
                write_json(
                    step_meta_path,
                    build_step_meta(
                        step_key=step_key,
                        name=name,
                        script=script,
                        result=result,
                        output_path=out_path,
                    ),
                )
                step_meta_paths[step_key] = str(step_meta_path)
                status_icon = {"ok": "✅", "error": "❌", "timeout": "⏰"}.get(result["status"], "?")
                logger.info("  %s %s: %s items (%ss)", status_icon, result["name"], result["count"], result["elapsed_s"])
                if result["status"] != "ok" and result["stderr_tail"]:
                    for line in result["stderr_tail"]:
                        logger.debug("    %s", line)

    fetch_elapsed = time.time() - t_start
    logger.info("📡 Fetch phase done in %.1fs", fetch_elapsed)

    logger.info("🔀 Merging & scoring...")
    merge_args = ["--verbose"] if args.verbose else []
    for flag, path in [
        ("--rss", tmp_rss),
        ("--twitter", tmp_twitter),
        ("--google", tmp_google),
        ("--github", tmp_github),
        ("--trending", tmp_trending),
        ("--api", tmp_api),
        ("--v2ex", tmp_v2ex),
        ("--reddit", tmp_reddit),
    ]:
        if path.exists():
            merge_args += [flag, str(path)]
    if args.archive_dir:
        merge_args += ["--archive-dir", str(args.archive_dir)]

    merge_result = run_step(
        "Merge",
        "merge-sources.py",
        merge_args,
        merged_output,
        timeout=MERGE_TIMEOUT,
        force=False,
        cooldown_s=None,
    )
    merge_meta_path = debug_dir / "merge.meta.json"
    write_json(
        merge_meta_path,
        build_step_meta(
            step_key="merge",
            name="Merge",
            script="merge-sources.py",
            result=merge_result,
            output_path=merged_output,
        ),
    )
    step_meta_paths["merge"] = str(merge_meta_path)

    if merge_result["status"] == "ok":
        logger.info("🧾 Rendering summary...")
        summarize_args = [
            "--input", str(merged_output),
            "--top", str(args.summary_top),
        ]
        summary_result = run_step(
            "Summarize",
            "merge-summarize.py",
            summarize_args,
            summary_output,
            timeout=SUMMARY_TIMEOUT,
            force=False,
            cooldown_s=None,
        )
    else:
        summary_result = {
            "name": "Summarize",
            "status": "skipped",
            "elapsed_s": 0,
            "count": 0,
            "effective_timeout_s": SUMMARY_TIMEOUT,
            "cooldown_s": None,
            "stderr_tail": [],
        }
    summarize_meta_path = debug_dir / "summarize.meta.json"
    write_json(
        summarize_meta_path,
        build_step_meta(
            step_key="summarize",
            name="Summarize",
            script="merge-summarize.py",
            result=summary_result,
            output_path=summary_output,
        ),
    )
    step_meta_paths["summarize"] = str(summarize_meta_path)

    total_elapsed = time.time() - t_start

    logger.info("%s", "=" * 50)
    logger.info("📊 Pipeline Summary (%.1fs total)", total_elapsed)
    for result in step_results:
        logger.info("   %-14s %-8s %4d items %6.1fs", result["name"], result["status"], result["count"], result["elapsed_s"])
    logger.info("   %-14s %-8s %4d items %6.1fs", "Merge", merge_result.get("status", "?"), merge_result.get("count", 0), merge_result.get("elapsed_s", 0))
    logger.info("   %-14s %-8s %4d items %6.1fs", "Summarize", summary_result.get("status", "?"), summary_result.get("count", 0), summary_result.get("elapsed_s", 0))
    logger.info("   Summary: %s", summary_output)
    logger.info("   Meta: %s", meta_output)
    logger.info("   Debug Dir: %s", debug_dir)

    meta = {
        "pipeline_version": "2.0.0",
        "debug_dir": str(debug_dir),
        "total_elapsed_s": round(total_elapsed, 1),
        "fetch_elapsed_s": round(fetch_elapsed, 1),
        "overall_status": (
            "error"
            if merge_result["status"] != "ok" or summary_result["status"] != "ok"
            else "ok"
        ),
        "steps": step_results,
        "step_meta_paths": step_meta_paths,
        "failed_items": build_pipeline_failed_items(step_results, merge_result, summary_result),
        "merge": merge_result,
        "summary_format": "json",
        "summary_status": summary_result.get("status"),
        "summary_elapsed_s": summary_result.get("elapsed_s"),
        "summary_output": str(summary_output),
    }
    write_json(meta_output, meta)

    removed_archive_dirs = cleanup_archive_root(args.archive_dir) if args.archive_dir else 0
    archived_outputs = archive_run_artifacts(args.archive_dir, summary_output, meta_output, step_meta_paths)
    if removed_archive_dirs:
        logger.info("🧹 Removed %d expired archive date directories", removed_archive_dirs)
    if archived_outputs.get("summary_json"):
        logger.info("🗂️  Archived summary JSON: %s", archived_outputs["summary_json"])
    if archived_outputs.get("pipeline_meta"):
        logger.info("🗂️  Archived meta dir: %s", archived_outputs.get("meta_dir"))

    meta["archive"] = {
        "root": str(args.archive_dir) if args.archive_dir else None,
        "retention_days": ARCHIVE_RETENTION_DAYS,
        "removed_expired_date_dirs": removed_archive_dirs,
        **archived_outputs,
    }
    write_json(meta_output, meta)

    if merge_result["status"] != "ok":
        logger.error("❌ Merge failed: %s", merge_result["stderr_tail"])
        return 1
    if summary_result["status"] != "ok":
        logger.error("❌ Summary failed: %s", summary_result["stderr_tail"])
        return 1

    logger.info("✅ Done → %s", summary_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
