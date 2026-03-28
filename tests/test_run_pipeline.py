#!/usr/bin/env python3
"""Tests for run-pipeline.py helpers."""

import importlib.util
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "run-pipeline.py"

spec = importlib.util.spec_from_file_location("run_pipeline", MODULE_PATH)
run_pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_pipeline)


class TestRunPipeline(unittest.TestCase):
    def test_make_process_result_supports_pending_status(self):
        step = run_pipeline.StepSpec("rss", "RSS", "fetch-rss.py", [], None)

        result = run_pipeline.make_process_result(
            spec=step,
            status="pending",
            timeout=1800,
        )

        self.assertEqual(result.status, "pending")
        self.assertEqual(result.effective_timeout_s, 1800)

    def test_run_step_process_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = Path(tmpdir) / "ok.py"
            output_path = Path(tmpdir) / "out.json"
            script_path.write_text(
                textwrap.dedent(
                    """
                    import json, sys
                    from pathlib import Path
                    out = Path(sys.argv[sys.argv.index("--output") + 1])
                    out.write_text(json.dumps({"total_articles": 3}), encoding="utf-8")
                    """
                ),
                encoding="utf-8",
            )
            step = run_pipeline.StepSpec("test", "Test", str(script_path), [], output_path)

            result = run_pipeline.run_step_process(step, timeout=10)

            self.assertEqual(result.status, "ok")

    def test_run_step_process_non_zero_exit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = Path(tmpdir) / "fail.py"
            output_path = Path(tmpdir) / "out.json"
            script_path.write_text("import sys\nsys.stderr.write('boom\\n')\nsys.exit(2)\n", encoding="utf-8")
            step = run_pipeline.StepSpec("test", "Test", str(script_path), [], output_path)

            result = run_pipeline.run_step_process(step, timeout=10)

            self.assertEqual(result.status, "error")
            self.assertIn("boom", result.stderr_tail[0])

    def test_run_step_process_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = Path(tmpdir) / "slow.py"
            output_path = Path(tmpdir) / "out.json"
            script_path.write_text("import time\ntime.sleep(2)\n", encoding="utf-8")
            step = run_pipeline.StepSpec("test", "Test", str(script_path), [], output_path)

            result = run_pipeline.run_step_process(step, timeout=1)

            self.assertEqual(result.status, "timeout")
            self.assertIn("Killed after 1s", result.stderr_tail[0])

    def test_run_step_process_exception_fallback(self):
        step = run_pipeline.StepSpec("test", "Test", "/definitely/missing.py", [], None)

        result = run_pipeline.run_step_process(step, timeout=1)

        self.assertEqual(result.status, "error")
        self.assertTrue(result.stderr_tail)

    def test_summarize_payload_details_extracts_record_summary(self):
        payload = {"total_articles": 4, "sources": [{"status": "ok"}, {"status": "error"}]}
        details = run_pipeline.summarize_payload_details(payload)
        self.assertEqual(details["record_summary"]["total"], 2)
        self.assertEqual(details["record_summary"]["error"], 1)

    def test_extract_call_stats_uses_google_query_stats(self):
        payload = {
            "topics": [
                {
                    "topic_id": "ai-models",
                    "status": "ok",
                    "query_stats": [
                        {"query": "openai", "status": "ok", "count": 4},
                        {"query": "anthropic", "status": "error", "count": 0, "error": "[error] site google/news: timeout"},
                    ],
                }
            ]
        }

        call_stats = run_pipeline.extract_call_stats(payload, step_key="google", status="error")

        self.assertEqual(call_stats, {"kind": "queries", "total_calls": 2, "ok_calls": 1, "failed_calls": 1})

    def test_extract_call_stats_prefers_explicit_top_level_call_fields(self):
        payload = {
            "calls_total": 6,
            "calls_ok": 4,
            "calls_kind": "queries",
            "topics": [
                {
                    "topic_id": "ai-models",
                    "status": "ok",
                    "query_stats": [{"query": "openai", "status": "ok", "count": 3}],
                }
            ],
        }

        call_stats = run_pipeline.extract_call_stats(payload, step_key="google", status="warn")

        self.assertEqual(call_stats, {"kind": "queries", "total_calls": 6, "ok_calls": 4, "failed_calls": 2})

    def test_build_diagnostics_includes_merge_deduplication_details(self):
        payload = {
            "input_sources": {"total_input": 10},
            "processing": {"scoring_version": "2.0"},
            "output_stats": {"total_articles": 6},
        }
        result = run_pipeline.make_process_result(
            spec=run_pipeline.StepSpec("merge", "Merge", "merge-sources.py", [], None),
            status="ok",
            timeout=300,
            elapsed_s=1.2,
        )

        meta = run_pipeline.build_diagnostics(payload, result, "merge")

        self.assertEqual(meta.details["processing"]["scoring_version"], "2.0")
        self.assertEqual(meta.details["deduplication"]["dropped"], 4)
        self.assertEqual(meta.items, 6)
        self.assertEqual(meta.call_stats["total_calls"], 1)

    def test_build_diagnostics_prefers_payload_errors(self):
        payload = {
            "sources": [
                {
                    "source_id": "simon-twitter",
                    "status": "error",
                    "error": "[error] site twitter/tweets: HTTP 429\n  Hint: queryId may have changed",
                }
            ]
        }
        result = run_pipeline.make_process_result(
            spec=run_pipeline.StepSpec("twitter", "Twitter", "fetch-twitter.py", [], None, 8.0),
            status="error",
            timeout=300,
            stderr_tail=["weak stderr"],
        )

        meta = run_pipeline.build_diagnostics(payload, result, "twitter")

        self.assertEqual(meta.failed_items[0]["id"], "simon-twitter")
        self.assertEqual(meta.failed_items[0]["error"], "[error] site twitter/tweets: HTTP 429 | Hint: queryId may have changed")

    def test_build_diagnostics_collects_google_query_errors_and_items(self):
        payload = {
            "total_articles": 192,
            "topics": [
                {
                    "topic_id": "ai-models",
                    "status": "ok",
                    "count": 20,
                    "query_stats": [
                        {"query": "openai", "status": "ok", "count": 20},
                        {
                            "query": "anthropic",
                            "status": "error",
                            "count": 0,
                            "error": "[error] site google/news: Error: Timed out loading Google news results\nReport: gh issue create ...",
                        },
                    ],
                }
            ],
        }
        result = run_pipeline.make_process_result(
            spec=run_pipeline.StepSpec("google", "Google News", "fetch-google.py", [], None, 12.0),
            status="error",
            timeout=300,
            elapsed_s=10.0,
            stderr_tail=["weak stderr"],
        )

        meta = run_pipeline.build_diagnostics(payload, result, "google")

        self.assertEqual(meta.items, 192)
        self.assertEqual(meta.call_stats["ok_calls"], 1)
        self.assertEqual(meta.call_stats["failed_calls"], 1)
        self.assertTrue(meta.failed_items[0]["error"].startswith("[error] site google/news: Error: Timed out loading Google news results"))

    def test_build_diagnostics_uses_step_aggregate_failure_when_payload_has_no_items(self):
        result = run_pipeline.make_process_result(
            spec=run_pipeline.StepSpec("merge", "Merge", "merge-sources.py", [], None),
            status="error",
            timeout=300,
            stderr_tail=["merge failed badly"],
        )

        meta = run_pipeline.build_diagnostics(None, result, "merge")

        self.assertEqual(meta.failed_items, [{"id": "__step__", "error": "merge failed badly"}])

    def test_build_diagnostics_does_not_emit_unknown_unknown_error(self):
        payload = {"sources": [{"source_id": "bad-rss", "status": "error", "error": ""}]}
        result = run_pipeline.make_process_result(
            spec=run_pipeline.StepSpec("rss", "RSS", "fetch-rss.py", [], None),
            status="error",
            timeout=300,
            stderr_tail=["rss timed out"],
        )

        meta = run_pipeline.build_diagnostics(payload, result, "rss")

        self.assertEqual(meta.failed_items, [{"id": "__step__", "error": "rss timed out"}])

    def test_build_pipeline_failed_items_uses_step_keys(self):
        failed_items = run_pipeline.build_pipeline_failed_items(
            [{"step_key": "twitter", "name": "Twitter", "status": "error", "stderr_tail": ["HTTP 429"], "failed_items": []}],
            [{"step_key": "summarize", "name": "Summarize", "status": "error", "stderr_tail": ["summary failed"], "failed_items": []}],
        )

        self.assertEqual(
            failed_items,
            [
                {"id": "twitter", "error": "HTTP 429"},
                {"id": "summarize", "error": "summary failed"},
            ],
        )

    def test_build_pipeline_failed_items_trims_errors(self):
        failed_items = run_pipeline.build_pipeline_failed_items(
            [{"step_key": "google", "name": "Google News", "status": "error", "stderr_tail": ["[error] site google/news: timeout\nHint: retry\nReport: noisy"], "failed_items": []}],
            [],
        )

        self.assertEqual(failed_items, [{"id": "google", "error": "[error] site google/news: timeout | Hint: retry"}])

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

    def test_archive_outputs_writes_json_and_meta_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            summary = root / "summary.json"
            pipeline_meta = root / "pipeline.meta.json"
            step_meta = root / "rss.meta.json"
            summary.write_text("{}", encoding="utf-8")
            pipeline_meta.write_text("{}", encoding="utf-8")
            step_meta.write_text("{}", encoding="utf-8")

            archived = run_pipeline.archive_outputs(root / "archive", summary, pipeline_meta, {"rss": str(step_meta)})

            self.assertTrue(Path(archived["summary_json"]).exists())
            self.assertTrue(Path(archived["pipeline_meta"]).exists())
            self.assertTrue(Path(archived["step_meta_paths"]["rss"]).exists())

    def test_archive_outputs_creates_archive_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            archive_root = root / "missing" / "archive"
            summary = root / "summary.json"
            pipeline_meta = root / "pipeline.meta.json"
            summary.write_text("{}", encoding="utf-8")
            pipeline_meta.write_text("{}", encoding="utf-8")

            archived = run_pipeline.archive_outputs(archive_root, summary, pipeline_meta, {})

            self.assertTrue(archive_root.exists())
            self.assertTrue(Path(archived["json_dir"]).exists())
            self.assertTrue(Path(archived["meta_dir"]).exists())


if __name__ == "__main__":
    unittest.main()
