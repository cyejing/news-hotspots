#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from unittest.mock import patch
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
    def test_build_google_query_quotes_multi_word_excludes(self):
        compiled = fetch_google.build_google_query("OpenAI launch", ["prompt engineering", "tutorial"])

        self.assertEqual(compiled, 'OpenAI launch -"prompt engineering" -tutorial')

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
            meta = fetch_google.build_step_meta_from_traces(
                step_key="google",
                status="error",
                elapsed_total_s=1.2,
                items=0,
                calls_total=2,
                calls_ok=0,
                failed_items=None,
                request_traces=[
                    {
                        "source_id": "q1",
                        "target": "openai",
                        "timing_s": {"active": 1.2, "total": 1.2},
                        "status": "error",
                        "source_type": "google",
                        "method": "CLI",
                        "attempt": 1,
                        "backend": "bb-browser",
                        "adapter": "google/news",
                        "error": "boom",
                    }
                ],
            )
            fetch_google.write_result_with_meta(output, result, meta)

            self.assertTrue(output.exists())
            self.assertTrue(output.with_suffix(".meta.json").exists())
            meta_payload = json.loads(output.with_suffix(".meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload["failed_items"][0]["source_id"], "q1")
            self.assertIn("request_timing_summary", meta_payload)
            self.assertIn("slow_requests", meta_payload)

    def test_fetch_topic_maps_snippet_to_summary_and_uses_local_timezone(self):
        topic = {"id": "ai", "search": {"google_queries": ["openai"]}}
        payload = {
            "results": [
                {
                    "title": "OpenAI launch",
                    "url": "https://example.com/openai",
                    "snippet": "New model shipped.",
                    "timestamp": 1775124000,
                    "source": "Example",
                },
                {
                    "title": "No summary",
                    "url": "https://example.com/no-summary",
                    "timestamp": 1775124000,
                    "source": "Example",
                },
            ]
        }

        with patch.object(fetch_google, "run_bb_browser_site", return_value=payload):
            result = fetch_google.fetch_topic(topic, fetch_google.logging.getLogger("test"))

        self.assertEqual(result["articles"][0]["summary"], "New model shipped.")
        self.assertEqual(result["articles"][1]["summary"], "")
        self.assertEqual(result["articles"][0]["source_name"], "Example")
        self.assertEqual(
            json.loads(json.dumps(result["articles"]))[0]["date"][-6:],
            fetch_google.local_now().strftime("%z")[:3] + ":" + fetch_google.local_now().strftime("%z")[3:],
        )

    def test_run_bb_browser_site_records_timeout_elapsed(self):
        with patch.object(fetch_google, "throttle_after_success", return_value=None), patch.object(
            fetch_google.subprocess,
            "run",
            side_effect=fetch_google.subprocess.TimeoutExpired(cmd=["bb-browser"], timeout=20),
        ):
            with self.assertRaises(fetch_google.TimedRuntimeError) as ctx:
                fetch_google.run_bb_browser_site(["google/news", "openai", "10"], timeout=20)

        self.assertEqual(ctx.exception.status, "timeout")
        self.assertIn("timed out after 20 seconds", str(ctx.exception))
        self.assertGreaterEqual(ctx.exception.elapsed_s, 0.0)

    def test_fetch_topic_preserves_timeout_status_and_elapsed_in_trace(self):
        topic = {"id": "ai", "search": {"google_queries": ["openai"]}}

        def raise_timeout(*args, **kwargs):
            raise fetch_google.TimedRuntimeError("timed out after 20 seconds", 20.0, status="timeout")

        with patch.object(fetch_google, "run_bb_browser_site", side_effect=raise_timeout):
            result = fetch_google.fetch_topic(topic, fetch_google.logging.getLogger("test"))

        self.assertEqual(result["request_traces"][0]["status"], "timeout")
        self.assertEqual(result["request_traces"][0]["timing_s"]["active"], 20.0)
        self.assertEqual(result["request_traces"][0]["timing_s"]["total"], 20.0)


if __name__ == "__main__":
    unittest.main()
