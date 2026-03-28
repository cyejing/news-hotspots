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
            "steps": [
                {"name": "RSS", "status": "ok"},
                {"name": "Twitter", "status": "skipped"},
            ],
            "merge": {"status": "ok", "count": 42, "stderr_tail": []},
            "summary_status": "ok",
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic["step_key"], "pipeline")
        self.assertEqual(diagnostic["state"], "warn")
        self.assertEqual(diagnostic["details"]["pipeline"]["skipped_steps"], ["Twitter"])

    def test_compute_step_state_marks_partial_record_failures_as_warn(self):
        meta = {
            "step_key": "rss",
            "name": "RSS",
            "status": "ok",
            "elapsed_s": 12.5,
            "count": 20,
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

        self.assertEqual(diagnostic["state"], "warn")
        self.assertEqual(diagnostic["failed_records"], 1)

    def test_compute_step_state_captures_merge_details(self):
        meta = {
            "step_key": "merge",
            "name": "Merge",
            "status": "ok",
            "elapsed_s": 3.2,
            "count": 90,
            "details": {
                "processing": {"scoring_version": "2.0"},
                "deduplication": {"input_total": 120, "output_total": 90, "dropped": 30, "drop_ratio": 0.25},
            },
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic["state"], "ok")
        self.assertEqual(diagnostic["details"]["deduplication"]["dropped"], 30)

    def test_compute_step_state_reads_failed_items_from_meta(self):
        meta = {
            "step_key": "twitter",
            "name": "Twitter",
            "status": "error",
            "elapsed_s": 3.2,
            "count": 0,
            "failed_items": [{"id": "simon-twitter", "error": "HTTP 429"}],
            "details": {"record_summary": {"kind": "sources", "total": 1, "ok": 0, "error": 1}},
        }

        diagnostic = source_health.compute_step_state(meta)

        self.assertEqual(diagnostic["failed_items"][0]["id"], "simon-twitter")
        self.assertEqual(diagnostic["failed_items"][0]["error"], "HTTP 429")

    def test_build_history_rows_keeps_latest_issue_summary(self):
        now = 1_800_000_000
        diagnostics = [
            {
                "step_key": "twitter",
                "name": "Twitter",
                "state": "error",
                "status": "error",
                "elapsed_s": 10.0,
                "count": 0,
                "failed_records": 1,
                "failed_items": [{"id": "simon-twitter", "error": "HTTP 429"}],
                "observed_ts": now - 100,
            },
            {
                "step_key": "twitter",
                "name": "Twitter",
                "state": "ok",
                "status": "ok",
                "elapsed_s": 8.0,
                "count": 10,
                "failed_records": 0,
                "failed_items": [],
                "observed_ts": now - 50,
            },
        ]

        rows = source_health.build_history_rows(diagnostics, now)

        self.assertEqual(rows[0]["latest_issue_summary"], "HTTP 429")
        self.assertEqual(rows[0]["check_details"][0]["count"], 10)
        self.assertEqual(rows[0]["check_details"][0]["failed_items"], [])
        self.assertEqual(rows[0]["check_details"][1]["failed_items"], [{"id": "simon-twitter", "error": "HTTP 429"}])

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
                        "count": 12,
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
                        "count": 10,
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
                        "count": 5,
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
                        "steps": [{"name": "Twitter", "status": "error", "stderr_tail": ["HTTP 429"]}],
                        "failed_items": [{"id": "twitter", "error": "HTTP 429"}],
                        "merge": {"status": "ok", "count": 1, "stderr_tail": []},
                        "summary_status": "ok",
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


if __name__ == "__main__":
    unittest.main()
