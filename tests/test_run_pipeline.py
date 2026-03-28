#!/usr/bin/env python3
"""Tests for run-pipeline.py helpers."""

import importlib.util
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "run-pipeline.py"

spec = importlib.util.spec_from_file_location("run_pipeline", MODULE_PATH)
run_pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_pipeline)


class TestRunPipeline(unittest.TestCase):
    def test_summarize_output_payload_extracts_record_summary(self):
        payload = {
            "total_articles": 4,
            "sources": [
                {"status": "ok"},
                {"status": "error"},
            ],
        }

        summary = run_pipeline.summarize_output_payload(payload)

        self.assertEqual(summary["record_summary"]["total"], 2)
        self.assertEqual(summary["record_summary"]["error"], 1)

    def test_build_step_meta_includes_merge_deduplication_details(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            merged_path = Path(tmpdir) / "merged.json"
            merged_path.write_text(
                """{
                  "input_sources": {"total_input": 10},
                  "processing": {"scoring_version": "2.0"},
                  "output_stats": {"total_articles": 6}
                }""",
                encoding="utf-8",
            )

            meta = run_pipeline.build_step_meta(
                step_key="merge",
                name="Merge",
                script="merge-sources.py",
                result={
                    "status": "ok",
                    "elapsed_s": 1.2,
                    "count": 6,
                    "effective_timeout_s": 300,
                    "cooldown_s": None,
                    "stderr_tail": [],
                },
                output_path=merged_path,
            )

            self.assertEqual(meta["details"]["processing"]["scoring_version"], "2.0")
            self.assertEqual(meta["details"]["deduplication"]["dropped"], 4)

    def test_build_step_meta_prefers_payload_errors_before_stderr_tail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "twitter.json"
            output_path.write_text(
                """{
                  "sources": [
                    {
                      "source_id": "simon-twitter",
                      "status": "error",
                      "error": "[error] site twitter/tweets: HTTP 429\\n  Hint: queryId may have changed"
                    }
                  ]
                }""",
                encoding="utf-8",
            )

            meta = run_pipeline.build_step_meta(
                step_key="twitter",
                name="Twitter",
                script="fetch-twitter.py",
                result={
                    "status": "error",
                    "elapsed_s": 1.2,
                    "count": 0,
                    "effective_timeout_s": 300,
                    "cooldown_s": 8.0,
                    "stderr_tail": [
                        'Report: gh issue create --repo epiral/bb-sites --title "[twitter/tweets] ..."',
                        "or: bb-browser site github/issue-create epir",
                    ],
                },
                output_path=output_path,
            )

            self.assertEqual(meta["failed_items"][0]["id"], "simon-twitter")
            self.assertEqual(
                meta["failed_items"][0]["error"],
                "[error] site twitter/tweets: HTTP 429\n  Hint: queryId may have changed",
            )

    def test_build_step_meta_uses_step_aggregate_failure_when_payload_has_no_items(self):
        meta = run_pipeline.build_step_meta(
            step_key="merge",
            name="Merge",
            script="merge-sources.py",
            result={
                "status": "error",
                "elapsed_s": 1.0,
                "count": 0,
                "effective_timeout_s": 300,
                "cooldown_s": None,
                "stderr_tail": ["merge failed badly"],
            },
            output_path=None,
        )

        self.assertEqual(meta["failed_items"], [{"id": "__step__", "error": "merge failed badly"}])

    def test_build_pipeline_failed_items_uses_step_keys(self):
        failed_items = run_pipeline.build_pipeline_failed_items(
            [{"step_key": "twitter", "name": "Twitter", "status": "error", "stderr_tail": ["HTTP 429"]}],
            {"status": "ok", "stderr_tail": []},
            {"status": "error", "stderr_tail": ["summary failed"]},
        )

        self.assertEqual(
            failed_items,
            [
                {"id": "twitter", "error": "HTTP 429"},
                {"id": "summarize", "error": "summary failed"},
            ],
        )

    def test_tmp_summary_path_stays_stable(self):
        resolved = run_pipeline.resolve_unique_output_path(Path("/tmp/summary.json"))
        self.assertEqual(resolved, Path("/tmp/summary.json"))

    def test_resolve_unique_output_path_appends_counter_suffix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "daily.json"
            path.write_text("{}", encoding="utf-8")
            (Path(tmpdir) / "daily1.json").write_text("{}", encoding="utf-8")

            resolved = run_pipeline.resolve_unique_output_path(path)

            self.assertEqual(resolved.name, "daily2.json")

    def test_cleanup_archive_root_removes_expired_date_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_root = Path(tmpdir)
            expired = archive_root / (datetime.now(timezone.utc) - timedelta(days=120)).strftime("%Y-%m-%d")
            fresh = archive_root / datetime.now(timezone.utc).strftime("%Y-%m-%d")
            expired.mkdir()
            fresh.mkdir()

            removed = run_pipeline.cleanup_archive_root(archive_root, retention_days=90)

            self.assertEqual(removed, 1)
            self.assertFalse(expired.exists())
            self.assertTrue(fresh.exists())

    def test_archive_run_artifacts_writes_json_and_meta_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            summary = root / "summary.json"
            pipeline_meta = root / "pipeline.meta.json"
            step_meta = root / "rss.meta.json"
            summary.write_text("{}", encoding="utf-8")
            pipeline_meta.write_text("{}", encoding="utf-8")
            step_meta.write_text("{}", encoding="utf-8")

            archived = run_pipeline.archive_run_artifacts(
                root / "archive",
                summary,
                pipeline_meta,
                {"rss": str(step_meta)},
            )

            self.assertTrue(Path(archived["summary_json"]).exists())
            self.assertTrue(Path(archived["pipeline_meta"]).exists())
            self.assertTrue(Path(archived["step_meta_paths"]["rss"]).exists())

    def test_archive_run_artifacts_creates_archive_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            archive_root = root / "missing" / "archive"
            summary = root / "summary.json"
            pipeline_meta = root / "pipeline.meta.json"
            summary.write_text("{}", encoding="utf-8")
            pipeline_meta.write_text("{}", encoding="utf-8")

            archived = run_pipeline.archive_run_artifacts(
                archive_root,
                summary,
                pipeline_meta,
                {},
            )

            self.assertTrue(archive_root.exists())
            self.assertTrue(Path(archived["json_dir"]).exists())
            self.assertTrue(Path(archived["meta_dir"]).exists())


if __name__ == "__main__":
    unittest.main()
