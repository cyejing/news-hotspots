#!/usr/bin/env python3
"""Tests for step_contract.py."""

import importlib.util
import unittest
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "step_contract.py"

spec = importlib.util.spec_from_file_location("step_contract", MODULE_PATH)
step_contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(step_contract)


class TestStepContract(unittest.TestCase):
    def test_now_iso_uses_local_timezone(self):
        value = step_contract.now_iso()
        parsed = datetime.fromisoformat(value)
        self.assertEqual(parsed.tzinfo, step_contract.local_tzinfo())

    def test_from_timestamp_local_uses_local_timezone(self):
        parsed = step_contract.from_timestamp_local(1775124000)
        self.assertEqual(parsed.tzinfo, step_contract.local_tzinfo())

    def test_build_step_meta_uses_bucketed_slow_requests_and_derived_failed_items(self):
        meta = step_contract.build_step_meta(
            step_key="twitter",
            status="partial",
            elapsed_active_s=8.0,
            elapsed_total_s=10.0,
            items=2,
            calls_total=2,
            calls_ok=1,
            request_traces=[
                {"source_id": "sama-twitter", "target": "@sama", "timing_s": {"active": 2.5, "total": 2.5}, "status": "ok", "source_type": "twitter", "method": "CLI", "backend": "bb-browser", "adapter": "twitter/tweets"},
                {"source_id": "openai-twitter", "target": "@openai", "timing_s": {"active": 6.2, "total": 7.0}, "status": "error", "source_type": "twitter", "method": "CLI", "attempt": 2, "backend": "bb-browser", "adapter": "twitter/tweets", "error": "boom"},
            ],
        )

        self.assertIn("request_timing_summary", meta)
        self.assertNotIn("timing_summary", meta)
        self.assertEqual(meta["request_timing_summary"]["requests_total"], 2)
        self.assertEqual(meta["timing_s"]["active"], 8.0)
        self.assertEqual(meta["timing_s"]["total"], 10.0)
        self.assertEqual(meta["failed_items"][0]["source_id"], "openai-twitter")
        self.assertEqual(meta["failed_items"][0]["attempt"], 2)
        self.assertEqual(meta["slow_requests"]["total_count"], 1)
        slow_bucket = next(bucket for bucket in meta["slow_requests"]["buckets"] if bucket["count"] == 1)
        self.assertEqual(slow_bucket["items"][0]["source_id"], "openai-twitter")
        self.assertEqual(slow_bucket["items"][0]["source_type"], "twitter")

    def test_write_result_with_meta_preserves_given_clean_payload(self):
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "twitter.json"
            step_contract.write_result_with_meta(
                output,
                {
                    "generated": "2026-04-02T10:00:00+08:00",
                    "source_type": "twitter",
                    "articles": [{"title": "A"}],
                },
                {"step_key": "twitter"},
            )

            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(sorted(payload.keys()), ["articles", "generated", "source_type"])


if __name__ == "__main__":
    unittest.main()
