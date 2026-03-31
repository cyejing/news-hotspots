#!/usr/bin/env python3
"""Tests for merge-hotspots.py and run-pipeline helpers."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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
    def _build_topic_articles(self):
        return [
            {
                "title": "Twitter Alpha",
                "topic": "ai-frontier",
                "source_name": "Twitter",
                "source_type": "twitter",
                "final_score": 15.0,
                "link": "https://x.com/alpha",
            },
            {
                "title": "Twitter Beta",
                "topic": "ai-frontier",
                "source_name": "Twitter",
                "source_type": "twitter",
                "final_score": 14.0,
                "link": "https://x.com/beta",
            },
            {
                "title": "RSS Alpha",
                "topic": "ai-frontier",
                "source_name": "Example RSS",
                "source_type": "rss",
                "final_score": 13.0,
                "link": "https://example.com/rss-alpha",
            },
            {
                "title": "Reddit Alpha",
                "topic": "ai-frontier",
                "source_name": "Reddit",
                "source_type": "reddit",
                "final_score": 12.0,
                "link": "https://reddit.com/r/test/alpha",
            },
            {
                "title": "GitHub Alpha",
                "topic": "ai-frontier",
                "source_name": "GitHub",
                "source_type": "github",
                "final_score": 11.0,
                "link": "https://github.com/org/repo",
            },
            {
                "title": "Twitter Gamma",
                "topic": "ai-frontier",
                "source_name": "Twitter",
                "source_type": "twitter",
                "final_score": 10.0,
                "link": "https://x.com/gamma",
            },
        ]

    def _build_source_type_payload(self, articles=None, total_articles=None):
        articles = articles or self._build_topic_articles()
        source_types = {}
        for article in articles:
            source_type = article["source_type"]
            source_types.setdefault(source_type, {"articles": []})
            source_types[source_type]["articles"].append(article)
        return {
            "generated": "2026-03-28T12:00:00+00:00",
            "output_stats": {"total_articles": total_articles or len(articles)},
            "source_types": source_types,
        }

    def test_build_hotspots_is_compact_json(self):
        data = self._build_source_type_payload(
            articles=[
                {
                    "title": "OpenAI ships a new model",
                    "topic": "ai-frontier",
                    "source_name": "OpenAI Blog",
                    "source_type": "rss",
                    "final_score": 12.3,
                    "link": "https://example.com/openai",
                    "snippet": "A short explanation of the release.",
                    "metrics": {"like_count": 999},
                    "date": "2026-03-28T11:00:00+00:00",
                }
            ],
            total_articles=2,
        )
        output = hotspots_mod.build_hotspots(data, top_n=5)
        self.assertEqual(output["total_articles"], 2)
        self.assertEqual(output["topic_order"], ["ai-frontier"])
        self.assertEqual(output["topics"][0]["title"], "Ai Frontier")
        self.assertEqual(output["topics"][0]["items"][0]["hotspot_score"], 12.3)
        self.assertEqual(output["topics"][0]["items"][0]["metrics"]["likes"], 999)
        self.assertNotIn("_score_components", output["topics"][0]["items"][0])

    def test_metrics_are_normalized(self):
        data = self._build_source_type_payload(
            articles=[
                {
                    "title": "OpenAI ships a new model",
                    "topic": "ai-frontier",
                    "source_name": "OpenAI Blog",
                    "source_type": "rss",
                    "final_score": 12.3,
                    "link": "https://example.com/openai",
                    "metrics": {"retweet_count": 50, "reply_count": 12},
                    "num_comments": 8,
                    "score": 123,
                }
            ]
        )
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
                    "title": "Ai Frontier",
                    "items": [
                        {
                            "rank": 1,
                            "hotspot_score": 12.3,
                            "title": "OpenAI ships a new model",
                            "link": "https://example.com/openai",
                            "source_name": "OpenAI Blog",
                            "metrics": {"likes": 999, "score": 123},
                        },
                        {
                            "rank": 2,
                            "hotspot_score": 10.0,
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
        self.assertIn("1. ⭐12.3 | [OpenAI ships a new model](https://example.com/openai)  ", markdown)
        self.assertIn("   来源：OpenAI Blog | 指标：likes=999, score=123", markdown)
        self.assertIn("   来源：The Verge", markdown)
        self.assertIn("## Ai Frontier\n1. ⭐12.3", markdown)

    def test_archive_pair_uses_matching_suffixes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            json_dir = root / "json"
            markdown_dir = root / "markdown"
            json_dir.mkdir()
            markdown_dir.mkdir()
            (json_dir / "daily.json").write_text("{}", encoding="utf-8")
            (markdown_dir / "daily.md").write_text("# sample\n", encoding="utf-8")

            json_path, markdown_path = hotspots_mod.resolve_archive_pair(json_dir, markdown_dir, stem="daily")

            self.assertEqual(json_path.name, "daily1.json")
            self.assertEqual(markdown_path.name, "daily1.md")

    def test_load_seen_daily_keys_ignores_merge_sources_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            json_dir = Path(tmpdir) / "json"
            json_dir.mkdir()
            daily_payload = {
                "topics": [
                    {
                        "items": [
                            {"title": "Seen Daily Story", "link": "https://example.com/daily"}
                        ]
                    }
                ]
            }
            merged_payload = {
                "source_types": {"rss": {"articles": [{"title": "Merged Candidate", "link": "https://example.com/merged"}]}}
            }
            (json_dir / "daily.json").write_text(json.dumps(daily_payload), encoding="utf-8")
            (json_dir / "merge-sources.json").write_text(json.dumps(merged_payload), encoding="utf-8")

            seen_titles, seen_links = hotspots_mod.load_seen_daily_keys(json_dir)

            self.assertIn(hotspots_mod.normalize_title_key("Seen Daily Story"), seen_titles)
            self.assertIn(hotspots_mod.normalize_link_key("https://example.com/daily"), seen_links)
            self.assertNotIn(hotspots_mod.normalize_title_key("Merged Candidate"), seen_titles)
            self.assertNotIn(hotspots_mod.normalize_link_key("https://example.com/merged"), seen_links)

    def test_debug_output_is_merge_hotspots_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = hotspots_mod.resolve_debug_output(Path(tmpdir))
            self.assertEqual(output, Path(tmpdir) / "merge-hotspots.json")
            self.assertTrue(Path(tmpdir).exists())

    def test_build_hotspots_debug_includes_ranking_details(self):
        data = self._build_source_type_payload(
            articles=[
                {
                    "title": "OpenAI ships a new model",
                    "topic": "ai-frontier",
                    "source_name": "OpenAI Blog",
                    "source_type": "rss",
                    "final_score": 12.34,
                    "link": "https://example.com/openai",
                    "scoring_debug": {"final_score": {"value": 12.34, "components": {"history_score": 0.0}}},
                }
            ]
        )

        hotspots = hotspots_mod.build_hotspots(data, top_n=5)
        debug_payload = hotspots_mod.build_hotspots_debug(data, hotspots, top_n=5)

        item = debug_payload["topics"][0]["items"][0]
        self.assertEqual(item["selection_debug"]["final_score"], 12.34)
        self.assertEqual(item["selection_debug"]["hotspot_score"], 12.3)
        self.assertIn("final_score_components", item["selection_debug"])
        self.assertTrue(item["selection_debug"]["same_day_dedup_applied"])
        self.assertTrue(item["selection_debug"]["source_type_first_pass"])

    def test_build_hotspots_uses_new_count_and_source_name_fields(self):
        data = self._build_source_type_payload(
            articles=[
                {
                    "title": "OpenAI ships a new model",
                    "topic": "ai-frontier",
                    "source_name": "OpenAI Blog",
                    "source_type": "rss",
                    "source_names": ["OpenAI Blog"],
                    "source_name_count": 1,
                    "final_score": 12.3,
                    "link": "https://example.com/openai",
                }
            ]
        )

        output = hotspots_mod.build_hotspots(data, top_n=5)
        self.assertEqual(output["topics"][0]["available_article_count"], 1)
        self.assertEqual(output["topics"][0]["remaining_article_count"], 1)
        self.assertEqual(output["topics"][0]["items"][0]["source_names"], ["OpenAI Blog"])
        self.assertEqual(output["topics"][0]["items"][0]["source_name_count"], 1)

    def test_build_hotspots_skips_same_day_seen_articles(self):
        data = self._build_source_type_payload(total_articles=6)

        output = hotspots_mod.build_hotspots(
            data,
            top_n=5,
            seen_titles={hotspots_mod.normalize_title_key("Twitter Alpha")},
            seen_links=set(),
        )

        titles = [item["title"] for item in output["topics"][0]["items"]]
        self.assertNotIn("Twitter Alpha", titles)
        self.assertEqual(len(titles), 5)

    def test_build_hotspots_prefers_distinct_source_types_then_fills(self):
        data = self._build_source_type_payload(total_articles=6)

        output = hotspots_mod.build_hotspots(data, top_n=5)
        titles = [item["title"] for item in output["topics"][0]["items"]]
        source_types = [item["source_type"] for item in output["topics"][0]["items"]]

        self.assertEqual(titles[:4], ["Twitter Alpha", "RSS Alpha", "Reddit Alpha", "GitHub Alpha"])
        self.assertEqual(source_types[:4], ["twitter", "rss", "reddit", "github"])
        self.assertEqual(len(titles), 5)
        self.assertEqual(titles[4], "Twitter Beta")

    def test_build_hotspots_allows_short_topic_when_candidates_exhausted(self):
        data = self._build_source_type_payload(
            articles=[
                {
                    "title": "Seen Story",
                    "topic": "ai-frontier",
                    "source_name": "Twitter",
                    "source_type": "twitter",
                    "final_score": 10.0,
                    "link": "https://x.com/seen",
                },
                {
                    "title": "Fresh Story",
                    "topic": "ai-frontier",
                    "source_name": "RSS",
                    "source_type": "rss",
                    "final_score": 9.0,
                    "link": "https://example.com/fresh",
                },
            ]
        )

        output = hotspots_mod.build_hotspots(
            data,
            top_n=5,
            seen_titles={hotspots_mod.normalize_title_key("Seen Story")},
            seen_links=set(),
        )

        self.assertEqual([item["title"] for item in output["topics"][0]["items"]], ["Fresh Story"])
        self.assertEqual(output["topics"][0]["remaining_article_count"], 1)

    def test_second_batch_uses_same_input_but_skips_first_daily_batch(self):
        data = self._build_source_type_payload(total_articles=6)

        first_batch = hotspots_mod.build_hotspots(data, top_n=5)
        seen_titles = {
            hotspots_mod.normalize_title_key(item["title"])
            for item in first_batch["topics"][0]["items"]
        }
        seen_links = {
            hotspots_mod.normalize_link_key(item["link"])
            for item in first_batch["topics"][0]["items"]
        }

        second_batch = hotspots_mod.build_hotspots(
            data,
            top_n=5,
            seen_titles=seen_titles,
            seen_links=seen_links,
        )

        self.assertEqual([item["title"] for item in second_batch["topics"][0]["items"]], ["Twitter Gamma"])

    def test_main_archives_merge_sources_and_next_daily_batch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "merge-sources.json"
            archive_root = root / "archive"
            payload = {
                "generated": "2026-03-28T12:00:00+00:00",
                "output_stats": {"total_articles": 6},
                "source_types": self._build_source_type_payload()["source_types"],
            }
            input_path.write_text(json.dumps(payload), encoding="utf-8")

            with mock.patch.object(
                sys,
                "argv",
                [
                    "merge-hotspots.py",
                    "--input",
                    str(input_path),
                    "--archive",
                    str(archive_root),
                    "--top",
                    "5",
                ],
            ):
                self.assertEqual(hotspots_mod.main(), 0)

            date_dir = next(archive_root.iterdir())
            json_dir = date_dir / "json"
            first_daily = json_dir / "daily.json"
            first_merged = json_dir / "merge-sources.json"
            self.assertTrue(first_daily.exists())
            self.assertTrue(first_merged.exists())

            with mock.patch.object(
                sys,
                "argv",
                [
                    "merge-hotspots.py",
                    "--input",
                    str(input_path),
                    "--archive",
                    str(archive_root),
                    "--top",
                    "5",
                ],
            ):
                self.assertEqual(hotspots_mod.main(), 0)

            second_daily = json_dir / "daily1.json"
            second_merged = json_dir / "merge-sources.json"
            self.assertTrue(second_daily.exists())
            self.assertTrue(second_merged.exists())
            self.assertFalse((json_dir / "merge-sources1.json").exists())

            first_payload = json.loads(first_daily.read_text(encoding="utf-8"))
            second_payload = json.loads(second_daily.read_text(encoding="utf-8"))
            first_titles = {item["title"] for item in first_payload["topics"][0]["items"]}
            second_titles = {item["title"] for item in second_payload["topics"][0]["items"]}
            self.assertTrue(first_titles)
            self.assertTrue(second_titles)
            self.assertTrue(first_titles.isdisjoint(second_titles))
            self.assertEqual(second_titles, {"Twitter Gamma"})

    def test_build_hotspots_rebuilds_topics_from_source_types(self):
        data = self._build_source_type_payload(
            articles=[
                {
                    "title": "Infra RSS",
                    "topic": "ai-infra",
                    "source_name": "Infra RSS",
                    "source_type": "rss",
                    "final_score": 9.0,
                    "link": "https://example.com/infra",
                },
                {
                    "title": "Frontier Twitter",
                    "topic": "ai-frontier",
                    "source_name": "Twitter",
                    "source_type": "twitter",
                    "final_score": 10.0,
                    "link": "https://x.com/frontier",
                },
            ],
            total_articles=2,
        )

        output = hotspots_mod.build_hotspots(data, top_n=5)

        self.assertEqual(output["topic_order"], ["ai-frontier", "ai-infra"])
        self.assertEqual([topic["id"] for topic in output["topics"]], ["ai-frontier", "ai-infra"])

    def test_build_hotspots_round_robins_then_backfills_same_source_type(self):
        data = self._build_source_type_payload(
            articles=[
                {
                    "title": "Twitter Alpha",
                    "topic": "ai-frontier",
                    "source_name": "Twitter",
                    "source_type": "twitter",
                    "final_score": 15.0,
                    "link": "https://x.com/alpha",
                },
                {
                    "title": "Twitter Beta",
                    "topic": "ai-frontier",
                    "source_name": "Twitter",
                    "source_type": "twitter",
                    "final_score": 14.0,
                    "link": "https://x.com/beta",
                },
                {
                    "title": "RSS Alpha",
                    "topic": "ai-frontier",
                    "source_name": "RSS",
                    "source_type": "rss",
                    "final_score": 13.0,
                    "link": "https://example.com/rss-alpha",
                },
            ]
        )

        output = hotspots_mod.build_hotspots(data, top_n=3)
        titles = [item["title"] for item in output["topics"][0]["items"]]

        self.assertEqual(titles, ["Twitter Alpha", "RSS Alpha", "Twitter Beta"])


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
