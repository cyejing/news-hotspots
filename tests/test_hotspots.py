#!/usr/bin/env python3
"""Tests for merge-hotspots.py and run-pipeline helpers."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


def load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hotspots_mod = load_module("merge_hotspots", "merge-hotspots.py")
run_pipeline_mod = load_module("run_pipeline", "run-pipeline.py")


class TestMergeHotspotsJson(unittest.TestCase):
    def test_build_hotspots_is_compact_json(self):
        data = {
            "generated": "2026-03-28T12:00:00+00:00",
            "output_stats": {"total_articles": 2},
            "topics": {
                "ai-models": {
                    "articles": [
                        {
                            "title": "OpenAI ships a new model",
                            "source_name": "OpenAI Blog",
                            "source_type": "rss",
                            "final_score": 12.3,
                            "link": "https://example.com/openai",
                            "snippet": "A short explanation of the release.",
                            "metrics": {"like_count": 999},
                            "date": "2026-03-28T11:00:00+00:00",
                        }
                    ]
                }
            },
        }
        output = hotspots_mod.build_hotspots(data, top_n=5)
        self.assertEqual(output["total_articles"], 2)
        self.assertEqual(output["topic_order"], ["ai-models"])
        self.assertEqual(output["topics"][0]["title"], "Ai Models")
        self.assertEqual(output["topics"][0]["items"][0]["score"], 12.3)
        self.assertEqual(output["topics"][0]["items"][0]["metrics"]["likes"], 999)
        self.assertNotIn("score_breakdown", output["topics"][0]["items"][0])

    def test_metrics_are_normalized(self):
        data = {
            "output_stats": {"total_articles": 1},
            "topics": {
                "ai-models": {
                    "articles": [
                        {
                            "title": "OpenAI ships a new model",
                            "source_name": "OpenAI Blog",
                            "source_type": "rss",
                            "final_score": 12.3,
                            "link": "https://example.com/openai",
                            "metrics": {"retweet_count": 50, "reply_count": 12},
                            "num_comments": 8,
                            "score": 123,
                        }
                    ]
                }
            },
        }
        output = hotspots_mod.build_hotspots(data, top_n=5)
        metrics = output["topics"][0]["items"][0]["metrics"]
        self.assertEqual(metrics["retweets"], 50)
        self.assertEqual(metrics["replies"], 12)
        self.assertEqual(metrics["comments"], 8)
        self.assertEqual(metrics["score"], 123)

    def test_build_markdown_uses_two_line_items(self):
        hotspots = {
            "generated_at": "2026-03-28T12:00:00+00:00",
            "topics": [
                {
                    "title": "Ai Models",
                    "items": [
                        {
                            "rank": 1,
                            "score": 12.3,
                            "title": "OpenAI ships a new model",
                            "link": "https://example.com/openai",
                            "source_name": "OpenAI Blog",
                            "metrics": {"likes": 999, "score": 123},
                        },
                        {
                            "rank": 2,
                            "score": 10.0,
                            "title": "Another model story",
                            "link": "https://example.com/other",
                            "source_name": "The Verge",
                            "metrics": {},
                        },
                    ],
                }
            ],
        }
        markdown = hotspots_mod.build_markdown(hotspots, mode="daily")
        self.assertIn("# 2026-03-28 daily 全球科技与 AI 热点", markdown)
        self.assertIn("- 1. ⭐12.3 | [OpenAI ships a new model](https://example.com/openai)", markdown)
        self.assertIn("  指标：likes=999, score=123 | 来源：OpenAI Blog", markdown)
        self.assertIn("  来源：The Verge", markdown)

    def test_archive_pair_uses_matching_suffixes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            json_dir = root / "json"
            markdown_dir = root / "markdown"
            json_dir.mkdir()
            markdown_dir.mkdir()
            (json_dir / "hotspots.json").write_text("{}", encoding="utf-8")
            (markdown_dir / "hotspots.md").write_text("# sample\n", encoding="utf-8")

            json_path, markdown_path = hotspots_mod.resolve_archive_pair(json_dir, markdown_dir)

            self.assertEqual(json_path.name, "hotspots1.json")
            self.assertEqual(markdown_path.name, "hotspots1.md")

    def test_debug_output_is_merge_hotspots_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = hotspots_mod.resolve_debug_output(Path(tmpdir))
            self.assertEqual(output, Path(tmpdir) / "merge-hotspots.json")
            self.assertTrue(Path(tmpdir).exists())


class TestDebugDirectoryResolution(unittest.TestCase):
    def test_debug_dir_wins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "run"
            run_dir = run_pipeline_mod.resolve_debug_dir(output_dir)
            self.assertEqual(run_dir, output_dir)
            self.assertTrue(run_dir.exists())

    def test_default_uses_temp_dir(self):
        run_dir = run_pipeline_mod.resolve_debug_dir(None)
        self.assertTrue(run_dir.exists())
        self.assertIn("news-hotspots-pipeline-", run_dir.name)


if __name__ == "__main__":
    unittest.main()
