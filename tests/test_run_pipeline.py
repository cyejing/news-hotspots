#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
MODULE_PATH = ROOT / "scripts" / "run-pipeline.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_pipeline", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_pipeline = load_module()


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

    def test_build_pipeline_meta_reports_partial_when_only_some_outputs_exist(self):
        meta = run_pipeline.build_pipeline_meta(
            runtime={"fetch": {}, "pipeline": {}},
            step_summaries={"rss": {"status": "ok"}, "google": {"status": "error"}},
            outputs={},
            archive_root=Path("/tmp/archive"),
            cleaned_archives=1,
            started_at=0.0,
        )
        self.assertEqual(meta["status"], "partial")
        self.assertEqual(meta["cleaned_archives"], 1)


if __name__ == "__main__":
    unittest.main()
