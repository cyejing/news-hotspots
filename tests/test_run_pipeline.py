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
    def test_run_step_process_exports_defaults_and_config_env(self):
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
            self.assertEqual(payload["defaults"], "/tmp/defaults")
            self.assertEqual(payload["config"], "/tmp/config")

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

    def test_build_pipeline_meta_reports_partial_when_only_some_outputs_exist(self):
        meta = run_pipeline.build_pipeline_meta(
            runtime={"fetch": {}, "pipeline": {}},
            step_summaries={
                "rss": {"status": "ok", "items": 3, "timing_s": {"active": 4.2, "total": 4.2}, "calls_total": 2, "calls_ok": 2, "failed_calls": 0, "failed_items": [], "slow_requests": {"total_count": 1}},
                "google": {"status": "error", "items": 0, "timing_s": {"active": 6.1, "total": 8.0}, "calls_total": 2, "calls_ok": 0, "failed_calls": 2, "failed_items": [{"source_id": "ai", "error": "boom"}], "slow_requests": {"total_count": 2}},
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
        rss = meta["source_type_overview"]["rss"]
        google = meta["source_type_overview"]["google"]
        self.assertTrue(rss["fully_successful"])
        self.assertEqual(rss["slow_requests_count"], 1)
        self.assertEqual(rss["timing_s"]["active"], 4.2)
        self.assertFalse(google["fully_successful"])
        self.assertEqual(google["failed_items_count"], 1)

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


if __name__ == "__main__":
    unittest.main()
