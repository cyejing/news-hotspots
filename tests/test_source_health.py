#!/usr/bin/env python3
"""Tests for source-health.py."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "source-health.py"

spec = importlib.util.spec_from_file_location("source_health", MODULE_PATH)
source_health = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source_health)


class TestSourceHealth(unittest.TestCase):
    def test_compute_step_state_supports_pipeline_meta(self):
        meta = {
            "pipeline_version": "2.0.0",
            "overall_status": "ok",
            "total_elapsed_s": 120.0,
            "fetch_elapsed_s": 119.0,
            "items": 42,
            "call_stats": {"kind": "steps", "total_calls": 4, "ok_calls": 3, "failed_calls": 0},
            "steps": [
                {"name": "RSS", "status": "ok"},
                {"name": "Twitter", "status": "skipped"},
            ],
            "merge": {"status": "ok", "count": 42, "stderr_tail": []},
            "hotspots_status": "ok",
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic.step_key, "pipeline")
        self.assertEqual(diagnostic.state, "warn")
        self.assertEqual(diagnostic.details["pipeline"]["skipped_steps"], ["Twitter"])
        self.assertEqual(diagnostic.items, 42)

    def test_compute_step_state_marks_partial_call_failures_as_warn(self):
        meta = {
            "step_key": "rss",
            "name": "RSS",
            "status": "ok",
            "elapsed_s": 12.5,
            "items": 20,
            "call_stats": {"kind": "sources", "total_calls": 5, "ok_calls": 4, "failed_calls": 1},
            "details": {
                "record_summary": {
                    "kind": "sources",
                    "total": 5,
                    "ok": 4,
                    "error": 1,
                }
            },
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic.state, "warn")
        self.assertEqual(diagnostic.call_stats["failed_calls"], 1)

    def test_compute_step_state_marks_pending_as_error(self):
        meta = {
            "step_key": "twitter",
            "name": "Twitter",
            "status": "pending",
            "elapsed_s": 0,
            "items": 0,
            "call_stats": {"kind": "sources", "total_calls": 0, "ok_calls": 0, "failed_calls": 0},
            "failed_items": [],
            "details": {},
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic.state, "error")

    def test_compute_step_state_captures_merge_details(self):
        meta = {
            "step_key": "merge",
            "name": "Merge",
            "status": "ok",
            "elapsed_s": 3.2,
            "items": 90,
            "call_stats": {"kind": "merge", "total_calls": 1, "ok_calls": 1, "failed_calls": 0},
            "details": {
                "processing": {"scoring_version": "2.0"},
                "deduplication": {"input_total": 120, "output_total": 90, "dropped": 30, "drop_ratio": 0.25},
            },
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic.state, "ok")
        self.assertEqual(diagnostic.details["deduplication"]["dropped"], 30)

    def test_compute_step_state_reads_failed_items_from_meta(self):
        meta = {
            "step_key": "twitter",
            "name": "Twitter",
            "status": "error",
            "elapsed_s": 3.2,
            "items": 0,
            "call_stats": {"kind": "sources", "total_calls": 1, "ok_calls": 0, "failed_calls": 1},
            "failed_items": [{"id": "simon-twitter", "error": "HTTP 429"}],
            "details": {},
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic.failed_items[0]["id"], "simon-twitter")
        self.assertEqual(diagnostic.failed_items[0]["error"], "HTTP 429")

    def test_build_history_rows_keeps_latest_issue_summary(self):
        now = 1_800_000_000
        diagnostics = [
            source_health.DiagnosticRecord(
                step_key="twitter",
                name="Twitter",
                state="error",
                status="error",
                elapsed_s=10.0,
                items=0,
                call_stats={"kind": "sources", "total_calls": 1, "ok_calls": 0, "failed_calls": 1},
                failed_items=[{"id": "simon-twitter", "error": "HTTP 429"}],
                details={},
                observed_ts=now - 100,
            ),
            source_health.DiagnosticRecord(
                step_key="twitter",
                name="Twitter",
                state="ok",
                status="ok",
                elapsed_s=8.0,
                items=10,
                call_stats={"kind": "sources", "total_calls": 1, "ok_calls": 1, "failed_calls": 0},
                failed_items=[],
                details={},
                observed_ts=now - 50,
            ),
        ]

        rows = source_health.build_history_rows(diagnostics, now)

        self.assertEqual(rows[0].latest_issue_summary, "HTTP 429")
        self.assertEqual(rows[0].check_details[0]["items"], 10)
        self.assertEqual(rows[0].check_details[0]["failed_items"], [])
        self.assertEqual(rows[0].check_details[1]["failed_items"], [{"id": "simon-twitter", "error": "HTTP 429"}])

    def test_main_reads_meta_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            (tmp_path / "rss.meta.json").write_text(
                json.dumps(
                    {
                        "step_key": "rss",
                        "name": "RSS",
                        "status": "ok",
                        "elapsed_s": 11.0,
                        "items": 12,
                        "call_stats": {"kind": "sources", "total_calls": 3, "ok_calls": 3, "failed_calls": 0},
                        "details": {
                            "record_summary": {"kind": "sources", "total": 3, "ok": 3, "error": 0}
                        },
                    }
                ),
                encoding="utf-8",
            )
            (tmp_path / "merge.meta.json").write_text(
                json.dumps(
                    {
                        "step_key": "merge",
                        "name": "Merge",
                        "status": "ok",
                        "elapsed_s": 2.4,
                        "items": 10,
                        "call_stats": {"kind": "merge", "total_calls": 1, "ok_calls": 1, "failed_calls": 0},
                        "details": {
                            "processing": {"scoring_version": "2.0"},
                            "deduplication": {"input_total": 12, "output_total": 10, "dropped": 2, "drop_ratio": 0.167},
                        },
                    }
                ),
                encoding="utf-8",
            )

            old_argv = source_health.sys.argv
            try:
                source_health.sys.argv = [
                    "source-health.py",
                    "--input-dir",
                    str(tmp_path),
                ]
                self.assertEqual(source_health.main(), 0)
            finally:
                source_health.sys.argv = old_argv

    def test_discover_archive_meta_files_reads_recent_meta_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_root = Path(tmpdir)
            recent_dir = archive_root / (source_health.datetime.now(source_health.timezone.utc) - source_health.timedelta(days=1)).strftime("%Y-%m-%d") / "meta"
            old_dir = archive_root / (source_health.datetime.now(source_health.timezone.utc) - source_health.timedelta(days=30)).strftime("%Y-%m-%d") / "meta"
            recent_dir.mkdir(parents=True)
            old_dir.mkdir(parents=True)
            (recent_dir / "pipeline.meta.json").write_text("{}", encoding="utf-8")
            (recent_dir / "rss.meta2.json").write_text("{}", encoding="utf-8")
            (old_dir / "rss.meta.json").write_text("{}", encoding="utf-8")

            files = source_health.discover_archive_meta_files(archive_root, days=7)

            names = [path.name for path in files]
            self.assertIn("pipeline.meta.json", names)
            self.assertIn("rss.meta2.json", names)
            self.assertEqual(len(files), 2)

    def test_discover_all_meta_files_combines_direct_and_archive_meta(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir)
            (input_dir / "pipeline.meta.json").write_text("{}", encoding="utf-8")
            recent_meta_dir = input_dir / "2026-03-28" / "meta"
            recent_meta_dir.mkdir(parents=True)
            (recent_meta_dir / "rss.meta2.json").write_text("{}", encoding="utf-8")

            files = source_health.discover_all_meta_files(input_dir, days=7)

            names = [path.name for path in files]
            self.assertIn("pipeline.meta.json", names)
            self.assertIn("rss.meta2.json", names)

    def test_parse_archive_run_label_uses_date_and_suffix_index(self):
        base = Path("/tmp/archive/2026-03-28/meta/pipeline.meta.json")
        second = Path("/tmp/archive/2026-03-28/meta/pipeline.meta1.json")

        self.assertEqual(source_health.parse_archive_run_label(base), "2026-03-28-1")
        self.assertEqual(source_health.parse_archive_run_label(second), "2026-03-28-2")

    def test_main_uses_input_dir_as_unified_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir)

            (input_dir / "rss.meta.json").write_text(
                json.dumps(
                    {
                        "step_key": "rss",
                        "name": "RSS",
                        "status": "ok",
                        "elapsed_s": 10.0,
                        "items": 5,
                        "call_stats": {"kind": "sources", "total_calls": 1, "ok_calls": 1, "failed_calls": 0},
                        "details": {"record_summary": {"kind": "sources", "total": 1, "ok": 1, "error": 0}},
                    }
                ),
                encoding="utf-8",
            )
            recent_meta_dir = input_dir / "2026-03-28" / "meta"
            recent_meta_dir.mkdir(parents=True)
            (recent_meta_dir / "pipeline.meta.json").write_text(
                json.dumps(
                    {
                        "pipeline_version": "2.0.0",
                        "overall_status": "error",
                        "total_elapsed_s": 10.0,
                        "fetch_elapsed_s": 9.0,
                        "items": 1,
                        "call_stats": {"kind": "steps", "total_calls": 3, "ok_calls": 2, "failed_calls": 1},
                        "steps": [{"name": "Twitter", "status": "error", "stderr_tail": ["HTTP 429"]}],
                        "failed_items": [{"id": "twitter", "error": "HTTP 429"}],
                        "merge": {"status": "ok", "count": 1, "stderr_tail": []},
                        "hotspots_status": "ok",
                    }
                ),
                encoding="utf-8",
            )

            old_argv = source_health.sys.argv
            try:
                source_health.sys.argv = [
                    "source-health.py",
                    "--input-dir",
                    str(input_dir),
                ]
                self.assertEqual(source_health.main(), 0)
            finally:
                source_health.sys.argv = old_argv

    def test_render_history_report_aligns_step_names(self):
        rows = [
            source_health.HistoryRow(
                step_key="rss",
                name="RSS",
                checks=2,
                ok=2,
                warn=0,
                error=0,
                degraded_rate=0.0,
                unhealthy=False,
                median_elapsed_s=2.0,
                latest_issue_ts=None,
                latest_issue_summary="",
                check_details=[],
            ),
            source_health.HistoryRow(
                step_key="google",
                name="Google News",
                checks=2,
                ok=1,
                warn=1,
                error=0,
                degraded_rate=0.5,
                unhealthy=False,
                median_elapsed_s=2.0,
                latest_issue_ts=None,
                latest_issue_summary="",
                check_details=[],
            ),
        ]

        output = "\n".join(source_health.render_history_report(rows))
        self.assertIn("✅ RSS         - ok:2 warn:0 error:0", output)
        self.assertIn("⚠️ Google News - ok:1 warn:1 error:0", output)

    def test_render_run_details_uses_calls_items_and_trimmed_errors(self):
        diagnostics = [
            source_health.DiagnosticRecord(
                run_label="2026-03-28-2",
                step_key="google",
                name="Google News",
                state="warn",
                status="ok",
                elapsed_s=583.1,
                items=192,
                call_stats={"kind": "queries", "total_calls": 11, "ok_calls": 10, "failed_calls": 1},
                failed_items=[
                    {
                        "id": "ai-models",
                        "error": "[error] site google/news: Error: Timed out loading Google news results\nReport: very noisy tail",
                    }
                ],
                details={},
                observed_ts=0,
            )
        ]

        output = "\n".join(source_health.render_run_details(diagnostics))
        self.assertIn("⚠️ Google News - calls:10/1/11 | items:192 | elapsed:583.1s", output)
        self.assertIn("ai-models: [error] site google/news: Error: Timed out loading Google news results | Report: very noisy tail", output)


if __name__ == "__main__":
    unittest.main()
