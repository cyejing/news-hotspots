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
    def test_debug_helpers_build_stable_default_shapes(self):
        article = {}

        similarity_debug = merge_sources.ensure_similarity_debug(article)
        scoring_debug = merge_sources.ensure_scoring_debug(article)

        self.assertEqual(similarity_debug["history_similarity"], 0.0)
        self.assertFalse(similarity_debug["history_duplicate"])
        self.assertEqual(similarity_debug["duplicate_group"]["cluster_size"], 1)
        self.assertEqual(similarity_debug["cross_source_hot"]["matched_source_type_count"], 0)
        self.assertEqual(similarity_debug["cross_source_hot"]["score"], 0.0)
        self.assertIn("final_score_formula", scoring_debug)

    def test_project_article_output_uses_stable_score_component_projection(self):
        article = {
            "title": "OpenAI releases GPT-5",
            "link": "https://example.com/1",
            "topic": "ai-frontier",
            "source_type": "rss",
            "source_name": "RSS A",
            "source_id": "rss-a",
            "source_priority": 3,
            "final_score": 8.5,
            "score_components": {
                "base_priority_score": 3.0,
                "fetch_local_rank_score": 2.5,
                "history_score": -1.0,
                "cross_source_hot_score": 2.0,
                "recency_score": 1.0,
                "local_extra_score": 0.5,
                "unused_field": 99,
            },
        }

        projected = merge_sources.project_article_output(article)

        self.assertEqual(projected["final_score"], 8.5)
        self.assertEqual(
            projected["score_components"],
            {
                "base_priority_score": 3.0,
                "fetch_local_rank_score": 2.5,
                "history_score": -1.0,
                "cross_source_hot_score": 2.0,
                "recency_score": 1.0,
                "local_extra_score": 0.5,
            },
        )
        self.assertNotIn("unused_field", projected["score_components"])
        self.assertNotIn("_score_components", projected)
        self.assertNotIn("_work_state", projected)

    def test_sanitize_article_record_strips_internal_work_fields(self):
        article = {
            "title": "OpenAI releases GPT-5",
            "_work_state": {"score_components": {}, "similarity_features": {}},
        }

        sanitized = merge_sources.sanitize_article_record(article)

        self.assertEqual(sanitized["title"], "OpenAI releases GPT-5")
        self.assertNotIn("_work_state", sanitized)

    def test_build_working_articles_creates_isolated_work_records(self):
        articles = [
            {
                "title": "OpenAI releases GPT-5",
                "link": "https://example.com/1",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_name": "RSS A",
                "source_id": "rss-a",
                "source_priority": 3,
            }
        ]

        working_articles = merge_sources.build_working_articles(articles)

        self.assertEqual(len(working_articles), 1)
        self.assertIn("_work_state", working_articles[0])
        self.assertIn("final_score", working_articles[0])
        self.assertNotIn("_work_state", articles[0])
        self.assertNotIn("final_score", articles[0])

    def test_export_score_components_fills_missing_keys(self):
        article = {
            "score_components": {
                "base_priority_score": 3,
                "local_extra_score": 1,
            },
        }

        exported = merge_sources.export_score_components(article)

        self.assertEqual(exported["base_priority_score"], 3.0)
        self.assertEqual(exported["local_extra_score"], 1.0)
        self.assertEqual(exported["fetch_local_rank_score"], 0.0)
        self.assertEqual(exported["history_score"], 0.0)
        self.assertEqual(exported["cross_source_hot_score"], 0.0)
        self.assertEqual(exported["recency_score"], 0.0)

    def test_ensure_scoring_debug_adds_component_semantics(self):
        article = {}

        scoring_debug = merge_sources.ensure_scoring_debug(article)

        self.assertEqual(
            scoring_debug["score_component_semantics"]["reference_only"],
            ["local_extra_score"],
        )
        self.assertIn("不直接计入 final_score", scoring_debug["local_extra_score_note"])

    def test_set_final_score_debug_populates_expected_component_fields(self):
        article = {"final_score": 8.5}
        score_components = {
            "base_priority_score": 3.0,
            "fetch_local_rank_score": 2.5,
            "history_score": -1.0,
            "cross_source_hot_score": 2.0,
            "recency_score": 1.0,
            "local_extra_score": 0.5,
        }

        merge_sources.set_final_score_debug(article, score_components)

        self.assertEqual(article["scoring_debug"]["final_score"]["value"], 8.5)
        self.assertEqual(
            article["scoring_debug"]["final_score"]["components"],
            score_components,
        )
        self.assertEqual(
            article["scoring_debug"]["final_score"]["component_membership"]["included_in_final_score"],
            [
                "base_priority_score",
                "fetch_local_rank_score",
                "history_score",
                "cross_source_hot_score",
                "recency_score",
            ],
        )
        self.assertEqual(
            article["scoring_debug"]["final_score"]["component_membership"]["reference_only"],
            ["local_extra_score"],
        )

    def test_article_field_helpers_normalize_business_fields(self):
        article = {"priority": 9}

        merge_sources.normalize_article_source_priority(article)
        merge_sources.set_article_topic(article, " ai-frontier ")
        merge_sources.set_article_multi_source(article, 1)
        merge_sources.set_article_final_score(article, 8.5678)

        self.assertEqual(article["source_priority"], merge_sources.normalize_priority(9))
        self.assertEqual(article["topic"], "ai-frontier")
        self.assertTrue(article["multi_source"])
        self.assertEqual(article["final_score"], 8.568)

    def test_ensure_similarity_features_uses_work_state(self):
        article = {
            "title": "OpenAI releases GPT-5",
            "link": "https://example.com/1",
        }

        features = merge_sources.ensure_similarity_features(article)

        self.assertEqual(features["normalized_title"], "openai releases gpt 5")
        self.assertIn("_work_state", article)
        self.assertEqual(article["_work_state"]["similarity_features"]["normalized_title"], "openai releases gpt 5")

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
        self.assertIn("score_components", result[0])
        self.assertNotIn("_work_state", articles[0])
        self.assertNotIn("final_score", articles[0])

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

    def test_history_similarity_keeps_exact_match_with_index_pruning(self):
        article = {
            "title": "OpenAI releases GPT-5 for enterprise coding",
            "link": "https://example.com/current",
            "topic": "ai-frontier",
            "source_type": "rss",
            "source_name": "RSS A",
                "source_id": "rss-a",
                "source_priority": 3,
        }
        previous_index = merge_sources.build_previous_title_index(
            [
                "Daily AI roundup and analysis",
                "OpenAI releases GPT-5 for enterprise coding",
                "Other unrelated title",
            ]
        )

        similarity = merge_sources.best_history_similarity(article, previous_index)
        self.assertGreaterEqual(similarity, 0.96)

    def test_noise_filter_drops_tracking_promo_post_without_keyword_blacklist(self):
        article = {
            "title": "88VIP邀请 - https://m.tb.cn/h.ioWIQD6?tk=Dnp35UdOaHE MF168",
            "summary": "好友喊你开通88VIP啦 2 个帖子 - 2 位参与者 阅读完整话题",
            "link": "https://linux.do/t/topic/123",
            "source_type": "rss",
            "topic": "technology",
        }

        self.assertTrue(merge_sources.is_likely_promotional_noise(article))

    def test_noise_filter_keeps_real_news_with_numbers(self):
        article = {
            "title": "Pinterest 通过自动内存重试将 Spark 的 OOM 故障减少了 96%",
            "summary": "这篇文章介绍了生产环境容错策略和批处理系统调优方法。",
            "link": "https://www.infoq.cn/article/zIgDwX3KbGVGPsebIv76",
            "source_type": "rss",
            "topic": "technology",
        }

        self.assertFalse(merge_sources.is_likely_promotional_noise(article))

    def test_build_candidate_pairs_can_cross_topics(self):
        articles = [
            {"title": "OpenAI releases GPT-5", "link": "https://a.com/1", "topic": "ai-frontier"},
            {"title": "OpenAI releases GPT-5", "link": "https://b.com/2", "topic": "business"},
        ]
        features = [merge_sources.build_similarity_features(article) for article in articles]

        self.assertEqual(list(merge_sources.build_candidate_pairs(features)), [(0, 1)])

    def test_similarity_bucket_limits_shrink_on_small_machine(self):
        profile = merge_sources.MachineProfile(cpu_count=2, memory_gb=4.0, max_workers=1, batch_size=128)
        limits = merge_sources.similarity_bucket_limits(profile)
        self.assertLess(limits["max_word_bucket_size"], merge_sources.SIMILARITY_LIMITS["max_word_bucket_size"])
        self.assertLess(limits["max_cjk_bucket_size"], merge_sources.SIMILARITY_LIMITS["max_cjk_bucket_size"])

    def test_deduplicate_articles_keeps_exact_duplicates_even_with_large_common_bucket(self):
        articles = [
            {
                "title": "AI platform update 0",
                "link": "https://example.com/0",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_name": "RSS",
                "source_id": "rss-0",
                "source_priority": 3,
            },
            {
                "title": "OpenAI launches agent platform",
                "link": "https://example.com/a",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_name": "RSS A",
                "source_id": "rss-a",
                "source_priority": 3,
            },
            {
                "title": "OpenAI launches agent platform",
                "link": "https://example.com/b",
                "topic": "ai-frontier",
                "source_type": "twitter",
                "source_name": "@openai",
                "source_id": "twitter-a",
                "source_priority": 5,
            },
        ]
        for idx in range(130):
            articles.append(
                {
                    "title": f"AI platform update {idx + 1}",
                    "link": f"https://example.com/common-{idx + 1}",
                    "topic": "ai-frontier",
                    "source_type": "rss",
                    "source_name": "RSS",
                    "source_id": f"rss-common-{idx + 1}",
                    "source_priority": 3,
                }
            )

        result = merge_sources.deduplicate_articles(articles)
        matching = [item for item in result if item["title"] == "OpenAI launches agent platform"]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]["source_type"], "twitter")

    def test_deduplicate_articles_projects_cross_source_matches_from_merge_stage(self):
        articles = [
            {
                "title": "OpenAI launches agent platform",
                "link": "https://example.com/a",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_name": "RSS A",
                "source_id": "rss-a",
                "source_priority": 3,
            },
            {
                "title": "OpenAI launches agent platform",
                "link": "https://x.com/openai/status/1",
                "topic": "ai-frontier",
                "source_type": "twitter",
                "source_name": "@openai",
                "source_id": "twitter-a",
                "source_priority": 5,
            },
        ]

        result = merge_sources.deduplicate_articles(articles)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_type"], "twitter")
        self.assertIn("cross_source_matches", result[0])
        self.assertEqual(len(result[0]["cross_source_matches"]), 1)
        self.assertEqual(result[0]["cross_source_matches"][0]["source_type"], "rss")
        self.assertEqual(result[0]["cross_source_matches"][0]["title"], "OpenAI launches agent platform")

    def test_build_input_stats_uses_registry_order(self):
        payloads = {
            "rss": {"articles": [{}]},
            "twitter": {"articles": [{}, {}]},
            "github_trending": {"articles": [{}]},
        }
        stats = merge_sources.build_input_stats(payloads)
        self.assertEqual(list(stats["source_type_distribution"].keys()), list(merge_sources.STEP_KEYS))
        self.assertEqual(stats["source_type_distribution"]["rss"], 1)
        self.assertEqual(stats["source_type_distribution"]["twitter"], 2)
        self.assertEqual(stats["source_type_distribution"]["github_trending"], 1)

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
            self.assertEqual(merged_article["source_name"], "RSS One")
            self.assertNotIn("source_names", merged_article)
            self.assertNotIn("source_name_count", merged_article)
            self.assertNotIn("display_name", merged_article)
            self.assertIsInstance(merged_article.get("cross_source_matches", []), list)


if __name__ == "__main__":
    unittest.main()
