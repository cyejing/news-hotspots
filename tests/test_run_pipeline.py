#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import textwrap
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).parent.parent
MODULE_PATH = ROOT / "scripts" / "run-pipeline.py"
REGISTRY_PATH = ROOT / "scripts" / "step_registry.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_pipeline", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_pipeline = load_module()


def load_registry_module():
    spec = importlib.util.spec_from_file_location("step_registry", REGISTRY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


step_registry = load_registry_module()


class TestRunPipeline(unittest.TestCase):
    def test_run_step_process_does_not_export_defaults_and_config_env(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            script_path = tmp / "envcheck.py"
            output_path = tmp / "out.json"
            script_path.write_text(
                textwrap.dedent(
                    """
                    import json, os, sys
                    from pathlib import Path
                    out = Path(sys.argv[sys.argv.index("--output") + 1])
                    out.write_text(json.dumps({
                        "args": sys.argv[1:],
                        "defaults": os.environ.get("NEWS_HOTSPOTS_DEFAULTS_DIR"),
                        "config": os.environ.get("NEWS_HOTSPOTS_CONFIG_DIR"),
                    }), encoding="utf-8")
                    """
                ),
                encoding="utf-8",
            )
            spec = run_pipeline.StepSpec(
                step_key="test",
                name="Test",
                script_name=str(script_path),
                args=["--defaults", "/tmp/defaults", "--config", "/tmp/config", "--output", str(output_path)],
                output_path=output_path,
                timeout_s=10,
            )

            original_dir = run_pipeline.SCRIPTS_DIR
            run_pipeline.SCRIPTS_DIR = tmp
            try:
                result = run_pipeline.run_step_process(spec)
            finally:
                run_pipeline.SCRIPTS_DIR = original_dir

            self.assertEqual(result.status, "ok")
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertIn("--defaults", payload["args"])
            self.assertIn("/tmp/defaults", payload["args"])
            self.assertIn("--config", payload["args"])
            self.assertIn("/tmp/config", payload["args"])
            self.assertIsNone(payload["defaults"])
            self.assertIsNone(payload["config"])

    def test_run_step_process_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            script_path = tmp / "slow.py"
            script_path.write_text("import time\ntime.sleep(2)\n", encoding="utf-8")
            spec = run_pipeline.StepSpec(
                step_key="test",
                name="Test",
                script_name=str(script_path),
                args=[],
                output_path=None,
                timeout_s=1,
            )

            original_dir = run_pipeline.SCRIPTS_DIR
            run_pipeline.SCRIPTS_DIR = tmp
            try:
                result = run_pipeline.run_step_process(spec)
            finally:
                run_pipeline.SCRIPTS_DIR = original_dir

            self.assertEqual(result.status, "timeout")
            self.assertIn("Killed after 1s", result.stderr_tail[-1])

    def test_run_step_process_keeps_longer_stderr_tail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            script_path = tmp / "stderr.py"
            script_path.write_text(
                "import sys\nfor i in range(150):\n    print(f'err-{i}', file=sys.stderr)\nsys.exit(1)\n",
                encoding="utf-8",
            )
            spec = run_pipeline.StepSpec(
                step_key="test",
                name="Test",
                script_name=str(script_path),
                args=[],
                output_path=None,
                timeout_s=10,
            )

            original_dir = run_pipeline.SCRIPTS_DIR
            run_pipeline.SCRIPTS_DIR = tmp
            try:
                result = run_pipeline.run_step_process(spec)
            finally:
                run_pipeline.SCRIPTS_DIR = original_dir

            self.assertEqual(result.status, "error")
            self.assertEqual(len(result.stderr_tail), run_pipeline.PROCESS_LOG_TAIL_LINES)
            self.assertEqual(result.stderr_tail[0], "err-50")
            self.assertEqual(result.stderr_tail[-1], "err-149")

    def test_build_fetch_specs_uses_unified_step_keys(self):
        runtime = {"pipeline": {"fetch_step_timeout_s": 99}}
        specs = run_pipeline.build_fetch_step_specs(
            defaults_dir=Path("/tmp/defaults"),
            config_dir=Path("/tmp/config"),
            debug_dir=Path("/tmp/debug"),
            hours=48,
            verbose=False,
            force=False,
            runtime=runtime,
        )
        self.assertEqual(specs[0].step_key, "rss")
        self.assertEqual(specs[4].step_key, "github_trending")
        self.assertTrue(str(specs[0].output_path).endswith("rss.json"))
        self.assertEqual(specs[0].timeout_s, 99)

    def test_build_fetch_specs_match_registry_order(self):
        runtime = {"pipeline": {"fetch_step_timeout_s": 99}}
        specs = run_pipeline.build_fetch_step_specs(
            defaults_dir=Path("/tmp/defaults"),
            config_dir=Path("/tmp/config"),
            debug_dir=Path("/tmp/debug"),
            hours=48,
            verbose=False,
            force=False,
            runtime=runtime,
        )
        self.assertEqual([spec.step_key for spec in specs], list(step_registry.STEP_KEYS))

    def test_build_hotspots_step_spec_passes_defaults_and_config(self):
        spec = run_pipeline.build_hotspots_step_spec(
            defaults_dir=Path("/tmp/defaults"),
            config_dir=Path("/tmp/config"),
            debug_dir=Path("/tmp/debug"),
            archive_dir=Path("/tmp/archive"),
            mode="daily",
            top_n=15,
            runtime={"pipeline": {"hotspots_timeout_s": 77}},
        )
        self.assertEqual(spec.timeout_s, 77)
        self.assertEqual(
            spec.args[:6],
            [
                "--defaults",
                "/tmp/defaults",
                "--input",
                "/tmp/debug/merge-sources.json",
                "--archive",
                "/tmp/archive",
            ],
        )
        self.assertIn("--config", spec.args)
        self.assertEqual(spec.args[spec.args.index("--config") + 1], "/tmp/config")

    def test_build_pipeline_meta_reports_partial_when_only_some_outputs_exist(self):
        meta = run_pipeline.build_pipeline_meta(
            runtime={"fetch": {}, "pipeline": {}},
            step_summaries={
                "rss": {"step_key": "rss", "status": "ok", "items": 3, "timing_s": {"active": 4.2, "total": 4.2}, "calls_total": 2, "calls_ok": 2, "failed_calls": 0, "failed_items": [], "slow_requests": {"total_count": 1}},
                "google": {"step_key": "google", "status": "error", "items": 0, "timing_s": {"active": 6.1, "total": 8.0}, "calls_total": 2, "calls_ok": 0, "failed_calls": 2, "failed_items": [{"source_id": "ai", "error": "boom"}], "slow_requests": {"total_count": 2}},
            },
            outputs={},
            archive_root=Path("/tmp/archive"),
            cleaned_archives=1,
            started_at=0.0,
            fetch_elapsed_s=12.3,
        )
        self.assertEqual(meta["status"], "partial")
        self.assertEqual(meta["cleaned_archives"], 1)
        self.assertEqual(meta["fetch_timing_s"]["total"], 12.3)
        self.assertEqual(meta["fetch_active_elapsed_s"], 10.3)
        self.assertEqual(meta["timing_semantics"]["fetch_active_elapsed_s"], "兼容旧字段，等价于 fetch_timing_s.active。")
        self.assertNotIn("steps", meta)
        self.assertNotIn("step_key", meta["step_summaries"]["rss"])
        self.assertNotIn("step_key", meta["step_summaries"]["google"])
        rss = meta["source_type_overview"]["rss"]
        google = meta["source_type_overview"]["google"]
        self.assertTrue(rss["fully_successful"])
        self.assertEqual(rss["slow_requests_count"], 1)
        self.assertEqual(rss["timing_s"]["active"], 4.2)
        self.assertFalse(google["fully_successful"])
        self.assertEqual(google["failed_items_count"], 1)
        self.assertEqual(meta["call_stats"]["failed_calls"], 1)
        self.assertEqual(meta["call_stats"].get("partial_calls", 0), 0)

    def test_build_pipeline_meta_counts_partial_steps_as_non_ok_calls(self):
        meta = run_pipeline.build_pipeline_meta(
            runtime={"fetch": {}, "pipeline": {}},
            step_summaries={
                "rss": {"step_key": "rss", "status": "ok", "items": 3, "timing_s": {"active": 4.2, "total": 4.2}, "calls_total": 2, "calls_ok": 2, "failed_calls": 0, "failed_items": [], "slow_requests": {"total_count": 1}},
                "twitter": {"step_key": "twitter", "status": "partial", "items": 2, "timing_s": {"active": 6.1, "total": 8.0}, "calls_total": 4, "calls_ok": 3, "failed_calls": 1, "failed_items": [{"source_id": "x", "error": "boom"}], "slow_requests": {"total_count": 2}},
            },
            outputs={},
            archive_root=Path("/tmp/archive"),
            cleaned_archives=0,
            started_at=0.0,
            fetch_elapsed_s=12.3,
        )
        self.assertEqual(meta["status"], "partial")
        self.assertEqual(meta["call_stats"]["ok_calls"], 1)
        self.assertEqual(meta["call_stats"]["partial_calls"], 1)
        self.assertEqual(meta["call_stats"]["failed_calls"], 1)

    def test_build_pipeline_meta_counts_skipped_steps_as_non_ok_calls(self):
        meta = run_pipeline.build_pipeline_meta(
            runtime={"fetch": {}, "pipeline": {}},
            step_summaries={
                "rss": {"step_key": "rss", "status": "ok", "items": 3, "timing_s": {"active": 4.2, "total": 4.2}, "calls_total": 2, "calls_ok": 2, "failed_calls": 0, "failed_items": [], "slow_requests": {"total_count": 1}},
                "twitter": {"step_key": "twitter", "status": "skipped", "items": 0, "timing_s": {"active": 0.0, "total": 0.0}, "calls_total": 0, "calls_ok": 0, "failed_calls": 0, "failed_items": [], "slow_requests": {"total_count": 0}},
            },
            outputs={},
            archive_root=Path("/tmp/archive"),
            cleaned_archives=0,
            started_at=0.0,
            fetch_elapsed_s=4.2,
        )
        self.assertEqual(meta["status"], "partial")
        self.assertEqual(meta["call_stats"]["ok_calls"], 1)
        self.assertEqual(meta["call_stats"].get("partial_calls", 0), 0)
        self.assertEqual(meta["call_stats"]["failed_calls"], 1)

    def test_build_pipeline_meta_uses_wall_clock_for_top_level_timing(self):
        with patch.object(run_pipeline.time, "monotonic", return_value=112.3):
            meta = run_pipeline.build_pipeline_meta(
                runtime={"fetch": {}, "pipeline": {}},
                step_summaries={
                    "rss": {"step_key": "rss", "status": "ok", "items": 3, "timing_s": {"active": 4.2, "total": 4.2}, "calls_total": 2, "calls_ok": 2, "failed_calls": 0, "failed_items": [], "slow_requests": {"total_count": 1}},
                    "twitter": {"step_key": "twitter", "status": "error", "items": 0, "timing_s": {"active": 6.1, "total": 8.0}, "calls_total": 2, "calls_ok": 0, "failed_calls": 2, "failed_items": [{"source_id": "x", "error": "boom"}], "slow_requests": {"total_count": 2}},
                },
                outputs={},
                archive_root=Path("/tmp/archive"),
                cleaned_archives=0,
                started_at=100.0,
                fetch_elapsed_s=12.3,
            )
        self.assertEqual(meta["timing_s"]["active"], 12.3)
        self.assertEqual(meta["timing_s"]["total"], 12.3)
        self.assertEqual(meta["fetch_timing_s"]["active"], 10.3)
        self.assertEqual(meta["fetch_timing_s"]["total"], 12.3)
        self.assertIn("wall-clock", meta["timing_semantics"]["timing_s"])

    def test_summarize_merge_step_uses_single_merge_call_semantics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            output_path = tmp / "merge-sources.json"
            output_path.write_text(
                json.dumps(
                    {
                        "input_stats": {"total_articles": 10},
                        "output_stats": {"total_articles": 7},
                    }
                ),
                encoding="utf-8",
            )
            spec = run_pipeline.StepSpec(
                step_key="merge-sources",
                name="Merge Sources",
                script_name="merge-sources.py",
                args=[],
                output_path=output_path,
                timeout_s=300,
            )
            result = run_pipeline.ProcessResult(
                step_key="merge-sources",
                name="Merge Sources",
                status="ok",
                elapsed_s=1.2,
                timeout_s=300,
            )

            summary = run_pipeline.summarize_merge_step(spec, result)

        self.assertEqual(summary["items"], 7)
        self.assertEqual(summary["calls_total"], 1)
        self.assertEqual(summary["calls_ok"], 1)
        self.assertEqual(summary["failed_calls"], 0)
        self.assertEqual(summary["input_items"], 10)
        self.assertEqual(summary["output_items"], 7)

    def test_cleanup_archive_root_uses_local_dates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_root = Path(tmpdir)
            recent = archive_root / (run_pipeline.local_now() - timedelta(days=1)).strftime("%Y-%m-%d")
            old = archive_root / (run_pipeline.local_now() - timedelta(days=10)).strftime("%Y-%m-%d")
            recent.mkdir()
            old.mkdir()

            removed = run_pipeline.cleanup_archive_root(archive_root, retention_days=7)

        self.assertEqual(removed, 1)

    def test_archive_step_meta_uses_local_date_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            meta_file = root / "rss.meta.json"
            meta_file.write_text("{}", encoding="utf-8")

            archived = run_pipeline.archive_step_meta(meta_file, root / "archive")

            self.assertIsNotNone(archived)
            self.assertIn(run_pipeline.local_today_iso(), str(archived))

    def test_archive_step_meta_supports_pipeline_meta(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            meta_file = root / "pipeline.meta.json"
            meta_file.write_text("{}", encoding="utf-8")

            archived = run_pipeline.archive_step_meta(meta_file, root / "archive")

            self.assertIsNotNone(archived)
            self.assertTrue(str(archived).endswith("pipeline.meta.json"))
            self.assertIn(run_pipeline.local_today_iso(), str(archived))

    def test_archive_step_meta_increments_name_without_overwriting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            meta_file = root / "pipeline.meta.json"
            meta_file.write_text('{"run": 1}', encoding="utf-8")

            first_archived = run_pipeline.archive_step_meta(meta_file, root / "archive")
            meta_file.write_text('{"run": 2}', encoding="utf-8")
            second_archived = run_pipeline.archive_step_meta(meta_file, root / "archive")
            meta_file.write_text('{"run": 3}', encoding="utf-8")
            third_archived = run_pipeline.archive_step_meta(meta_file, root / "archive")

            self.assertEqual(first_archived.name, "pipeline.meta.json")
            self.assertEqual(second_archived.name, "pipeline1.meta.json")
            self.assertEqual(third_archived.name, "pipeline2.meta.json")
            self.assertEqual(first_archived.read_text(encoding="utf-8"), '{"run": 1}')
            self.assertEqual(second_archived.read_text(encoding="utf-8"), '{"run": 2}')
            self.assertEqual(third_archived.read_text(encoding="utf-8"), '{"run": 3}')


    def test_summarize_fetch_step_overrides_ok_process_log_with_meta_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            output_path = tmp / "google.json"
            output_path.write_text(json.dumps({"generated": "2026-04-02T00:00:00+08:00", "source_type": "google", "articles": [{"title": "a"}]}), encoding="utf-8")
            output_path.with_suffix(".meta.json").write_text(
                json.dumps(
                    {
                        "step_key": "google",
                        "status": "partial",
                        "timing_s": {"active": 20.0, "total": 40.0},
                        "items": 1,
                        "calls_total": 3,
                        "calls_ok": 2,
                        "failed_calls": 1,
                        "failed_items": [{"source_id": "q1", "status": "timeout", "timing_s": {"active": 20.0, "total": 20.0}, "error": "timed out after 20 seconds"}],
                        "slow_requests": {"total_count": 1},
                    }
                ),
                encoding="utf-8",
            )
            spec = run_pipeline.StepSpec(
                step_key="google",
                name="Google",
                script_name="fetch-google.py",
                args=[],
                output_path=output_path,
                timeout_s=2000,
            )
            result = run_pipeline.ProcessResult(
                step_key="google",
                name="Google",
                status="ok",
                elapsed_s=40.0,
                timeout_s=2000,
                stdout_tail=[],
                stderr_tail=[],
                stdout_lines=0,
                stderr_lines=0,
                command=["python3", "fetch-google.py"],
                returncode=0,
            )

            summary = run_pipeline.summarize_fetch_step(spec, result)

        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["logs"]["status"], "partial")
        self.assertIn("status=partial", summary["logs"]["summary"])


if __name__ == "__main__":
    unittest.main()
