#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / "scripts" / "merge-sources.py"


def load_module():
    spec = importlib.util.spec_from_file_location("merge_sources", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


merge_sources = load_module()


class TestMergeSources(unittest.TestCase):
    def test_deduplicate_articles_prefers_higher_score_source(self):
        articles = [
            {
                "title": "OpenAI releases GPT-5",
                "link": "https://a.com/1",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_name": "RSS A",
                "source_id": "rss-a",
                "source_priority": 3,
            },
            {
                "title": "OpenAI releases GPT-5!",
                "link": "https://b.com/2",
                "topic": "ai-frontier",
                "source_type": "twitter",
                "source_name": "@openai",
                "source_id": "twitter-openai",
                "source_priority": 5,
                "metrics": {"like_count": 1000, "retweet_count": 400},
            },
        ]
        result = merge_sources.deduplicate_articles(articles)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_type"], "twitter")
        self.assertIn("final_score", result[0])

    def test_group_by_source_types_sorts_by_final_score(self):
        grouped = merge_sources.group_by_source_types(
            [
                {"title": "A", "source_type": "rss", "final_score": 1},
                {"title": "B", "source_type": "rss", "final_score": 3},
                {"title": "C", "source_type": "twitter", "final_score": 2},
            ]
        )
        self.assertEqual([item["title"] for item in grouped["rss"]], ["B", "A"])
        self.assertEqual(grouped["twitter"][0]["title"], "C")

    def test_main_merges_standardized_articles_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            rss = tmp / "rss.json"
            twitter = tmp / "twitter.json"
            output = tmp / "merge-sources.json"
            archive = tmp / "archive"
            archive.mkdir()

            rss.write_text(
                json.dumps(
                    {
                        "generated": "2026-04-02T00:00:00+00:00",
                        "source_type": "rss",
                        "articles": [
                            {
                                "title": "Article 1",
                                "link": "https://example.com/1",
                                "date": "2026-04-02T00:00:00+00:00",
                                "topic": "ai-frontier",
                                "source_type": "rss",
                                "source_id": "rss-1",
                                "source_name": "RSS One",
                                "source_priority": 3,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            twitter.write_text(
                json.dumps(
                    {
                        "generated": "2026-04-02T00:00:00+00:00",
                        "source_type": "twitter",
                        "articles": [
                            {
                                "title": "Article 2",
                                "link": "https://x.com/a/status/1",
                                "date": "2026-04-02T00:00:00+00:00",
                                "topic": "ai-infra",
                                "source_type": "twitter",
                                "source_id": "twitter-1",
                                "source_name": "@a",
                                "source_priority": 4,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            rc = merge_sources.main.__wrapped__() if hasattr(merge_sources.main, "__wrapped__") else None
            self.assertIsNone(rc)

            import subprocess, sys
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--rss",
                    str(rss),
                    "--twitter",
                    str(twitter),
                    "--output",
                    str(output),
                    "--archive",
                    str(archive),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertIn("input_stats", payload)
            self.assertIn("output_stats", payload)
            self.assertIn("source_types", payload)
            self.assertNotIn("topic_groups", payload)
            self.assertIn("processing", payload)
            self.assertEqual(payload["input_stats"]["total_articles"], 2)

            scoring_config = payload["processing"]["scoring_config"]
            self.assertIn("fetch_rank_max_score", scoring_config)
            self.assertIn("history_threshold", scoring_config)
            self.assertIn("history_score_rules", scoring_config)
            self.assertIn("cross_source_hot_threshold", scoring_config)
            self.assertIn("duplicate_threshold", scoring_config)
            self.assertIn("cross_source_hot_score_per_extra_type", scoring_config)
            self.assertIn("cross_source_hot_score_cap", scoring_config)
            self.assertIn("recency_24h_score", scoring_config)
            self.assertIn("recency_6h_score", scoring_config)
            self.assertNotIn("topic_same_source_score", scoring_config)
            self.assertNotIn("topic_same_domain_score", scoring_config)
            self.assertNotIn("topic_first3_source_score", scoring_config)
            self.assertNotIn("topic_first3_domain_score", scoring_config)

            merged_article = payload["source_types"]["rss"]["articles"][0]
            self.assertIn("final_score", merged_article)
            self.assertIn("score_components", merged_article)
            self.assertIn("local_extra_score", merged_article["score_components"])
            self.assertNotIn("scoring_debug", merged_article)
            self.assertNotIn("similarity_debug", merged_article)
            self.assertNotIn("source_names", merged_article)
            self.assertNotIn("source_name_count", merged_article)


if __name__ == "__main__":
    unittest.main()
