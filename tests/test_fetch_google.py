#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / "scripts" / "fetch-google.py"


def load_module():
    spec = importlib.util.spec_from_file_location("fetch_google", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch_google = load_module()


class TestFetchGoogle(unittest.TestCase):
    def test_apply_runtime_config_updates_results_per_query(self):
        original_timeout = fetch_google.DEFAULT_TIMEOUT
        original_results = fetch_google.RESULTS_PER_QUERY
        original_cooldown = fetch_google.COOLDOWN_SECONDS
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                config_dir = Path(tmpdir)
                (config_dir / "news-hotspots-runtime.json").write_text(
                    json.dumps({"fetch": {"google": {"request_timeout_s": 55, "cooldown_s": 9, "results_per_query": 7}}}),
                    encoding="utf-8",
                )
                fetch_google.apply_runtime_config(ROOT / "config" / "defaults", config_dir)
            self.assertEqual(fetch_google.DEFAULT_TIMEOUT, 55)
            self.assertEqual(fetch_google.RESULTS_PER_QUERY, 7)
            self.assertEqual(fetch_google.COOLDOWN_SECONDS, 9)
        finally:
            fetch_google.DEFAULT_TIMEOUT = original_timeout
            fetch_google.RESULTS_PER_QUERY = original_results
            fetch_google.COOLDOWN_SECONDS = original_cooldown

    def test_meta_path_uses_sidecar_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "google.json"
            result = {"generated": "2026-04-02T00:00:00+00:00", "source_type": "google", "articles": []}
            meta = fetch_google.build_step_meta(
                step_key="google",
                status="error",
                elapsed_s=1.2,
                items=0,
                calls_total=2,
                calls_ok=0,
                failed_items=[{"id": "q1", "error": "boom", "elapsed_s": 1.2}],
                request_traces=[],
            )
            fetch_google.write_result_with_meta(output, result, meta)

            self.assertTrue(output.exists())
            self.assertTrue(output.with_suffix(".meta.json").exists())
            meta_payload = json.loads(output.with_suffix(".meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload["failed_items"][0]["id"], "q1")
            self.assertIn("timing_summary", meta_payload)
            self.assertIn("slow_requests", meta_payload)


if __name__ == "__main__":
    unittest.main()
