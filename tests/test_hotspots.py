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

        hotspots = merge_hotspots.build_hotspots(
            payload,
            top_n=2,
            topic_metadata={
                "ai-frontier": {"emoji": "🧠", "label": "AI Frontier / AI 前沿"},
                "ai-infra": {"emoji": "⚙️", "label": "AI Infra / AI 基础设施"},
            },
        )
        self.assertEqual(hotspots["source_type_counts"]["rss"], 2)
        self.assertEqual(hotspots["source_type_counts"]["twitter"], 1)
        self.assertEqual(hotspots["topics"][0]["id"], "ai-frontier")
        self.assertEqual(hotspots["topics"][0]["title"], "🧠 AI 前沿")
        self.assertEqual(len(hotspots["topics"][0]["items"]), 2)
        self.assertEqual(hotspots["topics"][0]["items"][0]["title"], "C")
        self.assertEqual(hotspots["topics"][0]["items"][0]["selection_debug"]["selection_order"], 2)
        self.assertEqual(hotspots["topics"][0]["items"][0]["selection_debug"]["display_rank"], 1)

    def test_markdown_summary_lists_source_type_counts(self):
        markdown = merge_hotspots.build_markdown(
            {
                "generated_at": "2026-04-02T00:00:00+00:00",
                "total_articles": 3,
                "source_type_counts": {"rss": 2, "twitter": 1},
                "topics": [],
            }
        )
        self.assertIn("---\nsummary: mode:daily | total_articles:3 | rss:2 | twitter:1 | generated_at:2026-04-02T00:00:00+00:00\n---", markdown)

    def test_markdown_item_renders_single_line_and_omits_empty_summary(self):
        markdown = merge_hotspots.build_markdown(
            {
                "generated_at": "2026-04-02T00:00:00+00:00",
                "total_articles": 2,
                "source_type_counts": {"rss": 1},
                "topics": [
                    {
                        "id": "social",
                        "title": "🫂 社会",
                        "items": [
                            {
                                "rank": 1,
                                "title": "A",
                                "summary": "B",
                                "source_type": "rss",
                                "source_name": "RSS",
                                "hotspot_score": 9.4,
                                "metrics": {"likes": 10},
                            },
                            {
                                "rank": 2,
                                "title": "Only Title",
                                "summary": "",
                                "source_type": "rss",
                                "source_name": "RSS",
                                "hotspot_score": 8.2,
                                "metrics": {},
                            },
                        ],
                    }
                ],
            }
        )
        self.assertIn("## 🫂 社会", markdown)
        self.assertIn("1. ⭐9.4 | A - B | *rss - RSS* | *likes=10*", markdown)
        self.assertIn("2. ⭐8.2 | Only Title | *rss - RSS*", markdown)
        self.assertNotIn("Only Title -  |", markdown)

    def test_build_hotspots_sorts_selected_items_by_score_after_round_robin(self):
        payload = {
            "generated": "2026-04-02T00:00:00+00:00",
            "output_stats": {"total_articles": 3},
            "source_types": {
                "rss": {
                    "count": 2,
                    "articles": [
                        {"title": "Low", "link": "https://example.com/low", "topic": "ai-frontier", "source_type": "rss", "source_name": "RSS", "final_score": 7.0},
                        {"title": "Lower", "link": "https://example.com/lower", "topic": "ai-frontier", "source_type": "rss", "source_name": "RSS", "final_score": 6.0},
                    ],
                },
                "twitter": {
                    "count": 1,
                    "articles": [
                        {"title": "High", "link": "https://x.com/high", "topic": "ai-frontier", "source_type": "twitter", "source_name": "@high", "final_score": 9.5},
                    ],
                },
            },
        }

        hotspots = merge_hotspots.build_hotspots(payload, top_n=2)

        items = hotspots["topics"][0]["items"]
        self.assertEqual([item["title"] for item in items], ["High", "Low"])
        self.assertEqual(items[0]["rank"], 1)
        self.assertEqual(items[0]["selection_debug"]["selection_order"], 2)
        self.assertEqual(items[0]["selection_debug"]["display_rank"], 1)
        self.assertEqual(items[1]["rank"], 2)
        self.assertEqual(items[1]["selection_debug"]["selection_order"], 1)
        self.assertEqual(items[1]["selection_debug"]["display_rank"], 2)

    def test_build_hotspots_total_articles_and_source_counts_match_displayed_items(self):
        payload = {
            "generated": "2026-04-02T00:00:00+00:00",
            "output_stats": {"total_articles": 4},
            "source_types": {
                "rss": {
                    "count": 3,
                    "articles": [
                        {"title": "A", "link": "https://example.com/a", "topic": "ai-frontier", "source_type": "rss", "source_name": "RSS", "final_score": 9.0},
                        {"title": "B", "link": "https://example.com/b", "topic": "ai-frontier", "source_type": "rss", "source_name": "RSS", "final_score": 8.0},
                        {"title": "C", "link": "https://example.com/c", "topic": "ai-frontier", "source_type": "rss", "source_name": "RSS", "final_score": 7.0},
                    ],
                },
                "twitter": {
                    "count": 1,
                    "articles": [
                        {"title": "D", "link": "https://x.com/d", "topic": "ai-frontier", "source_type": "twitter", "source_name": "@d", "final_score": 8.5},
                    ],
                },
            },
        }

        hotspots = merge_hotspots.build_hotspots(payload, top_n=2)

        self.assertEqual(hotspots["total_articles"], 2)
        self.assertEqual(hotspots["source_type_counts"], {"twitter": 1, "rss": 1})
        self.assertEqual(hotspots["candidate_total_articles"], 4)
        self.assertEqual(hotspots["candidate_source_type_counts"], {"rss": 3, "twitter": 1})

    def test_topic_display_title_uses_topics_json_metadata(self):
        self.assertEqual(
            merge_hotspots.topic_display_title(
                "ai-frontier",
                {"ai-frontier": {"emoji": "🧠", "label": "AI Frontier / AI 前沿"}},
            ),
            "🧠 AI 前沿",
        )
        self.assertEqual(
            merge_hotspots.topic_display_title(
                "social",
                {"social": {"emoji": "🫂", "label": "Social / 社会"}},
            ),
            "🫂 社会",
        )

    def test_script_archives_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            archive = root / "archive"
            defaults = root / "defaults"
            config = root / "config"
            input_path = root / "merge-sources.json"
            defaults.mkdir()
            config.mkdir()
            (defaults / "runtime.json").write_text(
                json.dumps({"pipeline": {"default_hotspots_top_n": 5}}),
                encoding="utf-8",
            )
            (defaults / "topics.json").write_text(
                json.dumps({"topics": [{"id": "ai-frontier", "emoji": "🧠", "label": "AI Frontier / AI 前沿"}]}),
                encoding="utf-8",
            )
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
                    "--defaults",
                    str(defaults),
                    "--config",
                    str(config),
                    "--input",
                    str(input_path),
                    "--archive",
                    str(archive),
                    "--mode",
                    "daily",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("ARCHIVED_JSON=", completed.stdout)
            self.assertIn("ARCHIVED_MARKDOWN=", completed.stdout)

            archived_json_line = next(line for line in completed.stdout.splitlines() if line.startswith("ARCHIVED_JSON="))
            archived_json_path = Path(archived_json_line.split("=", 1)[1].strip())
            archived_payload = json.loads(archived_json_path.read_text(encoding="utf-8"))
            archived_item = archived_payload["topics"][0]["items"][0]
            self.assertIn("score_debug", archived_item)
            self.assertEqual(archived_item["score_debug"]["formula_zh"], "基础优先级分 + 源内排序分 + 历史重复修正分 + 跨源共振分 + 时效性分")
            self.assertEqual(archived_item["score_debug"]["components"]["local_extra_score"], 0.5)
            self.assertIn("selection_debug", archived_item)
            self.assertEqual(archived_item["selection_debug"]["source_type_rank"], 1)
            self.assertEqual(archived_item["selection_debug"]["source_type_total_candidates"], 1)
            self.assertTrue(archived_item["selection_debug"]["selected_after_same_day_dedup"])
            self.assertTrue(archived_item["selection_debug"]["selected_by_round_robin"])
            self.assertEqual(archived_item["selection_debug"]["selection_order"], 1)
            self.assertEqual(archived_item["selection_debug"]["display_rank"], 1)
            self.assertIn("explanations_zh", archived_item["selection_debug"])
            self.assertNotIn("display_name", archived_item)
            self.assertNotIn("source_names", archived_item)
            self.assertNotIn("source_name_count", archived_item)


if __name__ == "__main__":
    unittest.main()
