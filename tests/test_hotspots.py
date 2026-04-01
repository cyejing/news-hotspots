#!/usr/bin/env python3

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / "scripts" / "merge-hotspots.py"


def load_module():
    spec = importlib.util.spec_from_file_location("merge_hotspots", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


merge_hotspots = load_module()


class TestMergeHotspots(unittest.TestCase):
    def test_build_hotspots_rebuilds_topics_from_source_types(self):
        payload = {
            "generated": "2026-04-02T00:00:00+00:00",
            "output_stats": {"total_articles": 3},
            "source_types": {
                "rss": {
                    "count": 2,
                    "articles": [
                        {"title": "A", "link": "https://example.com/a", "topic": "ai-frontier", "source_type": "rss", "source_name": "RSS", "final_score": 3},
                        {"title": "B", "link": "https://example.com/b", "topic": "ai-infra", "source_type": "rss", "source_name": "RSS", "final_score": 2},
                    ],
                },
                "twitter": {
                    "count": 1,
                    "articles": [
                        {"title": "C", "link": "https://x.com/c", "topic": "ai-frontier", "source_type": "twitter", "source_name": "@c", "final_score": 4},
                    ],
                },
            },
        }

        hotspots = merge_hotspots.build_hotspots(payload, top_n=2)
        self.assertEqual(hotspots["source_type_counts"]["rss"], 2)
        self.assertEqual(hotspots["source_type_counts"]["twitter"], 1)
        self.assertEqual(hotspots["topics"][0]["id"], "ai-frontier")
        self.assertEqual(len(hotspots["topics"][0]["items"]), 2)

    def test_markdown_summary_lists_source_type_counts(self):
        markdown = merge_hotspots.build_markdown(
            {
                "generated_at": "2026-04-02T00:00:00+00:00",
                "total_articles": 3,
                "source_type_counts": {"rss": 2, "twitter": 1},
                "topics": [],
            }
        )
        self.assertIn("## Summary", markdown)
        self.assertIn("- rss: 2", markdown)
        self.assertIn("- twitter: 1", markdown)

    def test_script_archives_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            archive = root / "archive"
            input_path = root / "merge-sources.json"
            debug_output = root / "debug" / "merge-hotspots.json"
            input_path.write_text(
                json.dumps(
                    {
                        "generated": "2026-04-02T00:00:00+00:00",
                        "output_stats": {"total_articles": 1},
                        "source_types": {
                            "rss": {
                                "count": 1,
                                "articles": [
                                    {
                                        "title": "A",
                                        "link": "https://example.com/a",
                                        "topic": "ai-frontier",
                                        "source_type": "rss",
                                        "source_name": "RSS",
                                        "source_priority": 3,
                                        "final_score": 3,
                                        "score_components": {
                                            "base_priority_score": 3.0,
                                            "fetch_local_rank_score": 2.0,
                                            "history_score": 0.0,
                                            "cross_source_hot_score": 0.0,
                                            "recency_score": 1.0,
                                            "local_extra_score": 0.5,
                                        },
                                    }
                                ],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--input",
                    str(input_path),
                    "--archive",
                    str(archive),
                    "--debug-output",
                    str(debug_output),
                    "--mode",
                    "daily",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("ARCHIVED_JSON=", completed.stdout)
            self.assertIn("ARCHIVED_MARKDOWN=", completed.stdout)
            self.assertTrue(debug_output.exists())

            debug_payload = json.loads(debug_output.read_text(encoding="utf-8"))
            debug_item = debug_payload["topics"][0]["items"][0]
            self.assertIn("score_debug", debug_item)
            self.assertEqual(debug_item["score_debug"]["formula_zh"], "基础优先级分 + 源内排序分 + 历史重复修正分 + 跨源共振分 + 时效性分")
            self.assertEqual(debug_item["score_debug"]["components"]["local_extra_score"], 0.5)
            self.assertIn("selection_debug", debug_item)
            self.assertEqual(debug_item["selection_debug"]["source_type_rank"], 1)
            self.assertEqual(debug_item["selection_debug"]["source_type_total_candidates"], 1)
            self.assertTrue(debug_item["selection_debug"]["selected_after_same_day_dedup"])
            self.assertTrue(debug_item["selection_debug"]["selected_by_round_robin"])
            self.assertIn("explanations_zh", debug_item["selection_debug"])

            archived_json_line = next(line for line in completed.stdout.splitlines() if line.startswith("ARCHIVED_JSON="))
            archived_json_path = Path(archived_json_line.split("=", 1)[1].strip())
            archived_payload = json.loads(archived_json_path.read_text(encoding="utf-8"))
            archived_item = archived_payload["topics"][0]["items"][0]
            self.assertNotIn("score_debug", archived_item)
            self.assertNotIn("selection_debug", archived_item)


if __name__ == "__main__":
    unittest.main()
