#!/usr/bin/env python3
"""Tests for merge-sources.py using real captured fixture data.

Run: python3 -m pytest tests/ -v
  or: python3 tests/test_merge.py
"""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))
FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Import merge-sources as module
import importlib.util
spec = importlib.util.spec_from_file_location("merge_sources", SCRIPTS_DIR / "merge-sources.py")
merge_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(merge_mod)

normalize_title = merge_mod.normalize_title
calculate_title_similarity = merge_mod.calculate_title_similarity
normalize_url_for_dedup = merge_mod.normalize_url
deduplicate_articles = merge_mod.deduplicate_articles
apply_domain_limits = merge_mod.apply_domain_limits
group_by_topics = merge_mod.group_by_topics
group_by_source_types = merge_mod.group_by_source_types
DOMAIN_LIMIT_EXEMPT = merge_mod.DOMAIN_LIMIT_EXEMPT
calculate_v2ex_replies_score = merge_mod.calculate_v2ex_replies_score
calculate_recency_score = merge_mod.calculate_recency_score
load_previous_hotspots = merge_mod.load_previous_hotspots


def load_fixture(name):
    with open(FIXTURES_DIR / f"{name}.json", "r") as f:
        return json.load(f)


class TestNormalizeTitle(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(normalize_title("  Hello World  "), "hello world")

    def test_empty(self):
        self.assertEqual(normalize_title(""), "")


class TestTitleSimilarity(unittest.TestCase):
    def test_identical(self):
        self.assertAlmostEqual(
            calculate_title_similarity("Hello World", "Hello World"), 1.0
        )

    def test_different(self):
        sim = calculate_title_similarity("Python 3.12 released", "Rust 1.75 announced")
        self.assertLess(sim, 0.5)

    def test_similar(self):
        sim = calculate_title_similarity(
            "OpenAI releases GPT-5 model", "OpenAI releases new GPT-5 model"
        )
        self.assertGreater(sim, 0.8)

    def test_length_diff_shortcut(self):
        sim = calculate_title_similarity("Short", "This is a much much longer title")
        self.assertLess(sim, 0.5)

    def test_chinese_similar(self):
        sim = calculate_title_similarity("OpenAI 发布 GPT-5 新模型", "OpenAI发布GPT5新模型")
        self.assertGreater(sim, 0.85)

    def test_mixed_language_similar(self):
        sim = calculate_title_similarity("Claude 4 API 发布", "Anthropic Claude-4 API正式发布")
        self.assertGreater(sim, 0.55)


class TestURLDedup(unittest.TestCase):
    def test_strips_query(self):
        url1 = normalize_url_for_dedup("https://example.com/article?ref=twitter")
        url2 = normalize_url_for_dedup("https://example.com/article?ref=rss")
        self.assertEqual(url1, url2)

    def test_strips_www(self):
        url1 = normalize_url_for_dedup("https://www.example.com/page")
        url2 = normalize_url_for_dedup("https://example.com/page")
        self.assertEqual(url1, url2)

    def test_strips_trailing_slash(self):
        url1 = normalize_url_for_dedup("https://example.com/page/")
        url2 = normalize_url_for_dedup("https://example.com/page")
        self.assertEqual(url1, url2)


class TestDeduplication(unittest.TestCase):
    def test_removes_url_duplicates(self):
        articles = [
            {"title": "Article A", "link": "https://example.com/a?ref=rss", "topic": "ai-frontier"},
            {"title": "Article A from RSS", "link": "https://example.com/a?ref=twitter", "topic": "ai-frontier"},
            {"title": "Article B", "link": "https://example.com/b", "topic": "ai-frontier"},
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 2)

    def test_removes_title_duplicates(self):
        articles = [
            {"title": "OpenAI releases GPT-5", "link": "https://a.com/1", "topic": "ai-frontier"},
            {"title": "OpenAI releases GPT-5!", "link": "https://b.com/2", "topic": "ai-frontier"},
            {"title": "Completely different article", "link": "https://c.com/3", "topic": "ai-frontier"},
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 2)

    def test_keeps_different_articles(self):
        articles = [
            {"title": "Python 3.12 released", "link": "https://a.com/1", "topic": "ai-frontier"},
            {"title": "Rust 1.75 announced", "link": "https://b.com/2", "topic": "ai-frontier"},
            {"title": "Go 1.22 is out", "link": "https://c.com/3", "topic": "ai-frontier"},
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 3)

    def test_prefers_highest_final_score_in_duplicate_cluster(self):
        articles = [
            {
                "title": "OpenAI releases GPT-5",
                "link": "https://a.com/1",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_priority": 3,
            },
            {
                "title": "OpenAI releases GPT-5!",
                "link": "https://b.com/2",
                "topic": "ai-frontier",
                "source_type": "twitter",
                "source_priority": 5,
                "metrics": {"like_count": 2000, "retweet_count": 800},
            },
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_type"], "twitter")
        self.assertIn("_score_components", result[0])
        self.assertIn("final_score", result[0])


class TestDomainLimits(unittest.TestCase):
    def test_limits_regular_domain(self):
        articles = [{"title": f"Article {i}", "link": f"https://techcrunch.com/art{i}"} for i in range(10)]
        result = apply_domain_limits(articles, max_per_domain=3)
        self.assertEqual(len(result), 3)

    def test_exempts_twitter(self):
        articles = [{"title": f"Tweet {i}", "link": f"https://x.com/user{i}/status/{i}"} for i in range(10)]
        result = apply_domain_limits(articles, max_per_domain=3)
        self.assertEqual(len(result), 10)

    def test_exempts_github(self):
        articles = [{"title": f"Release {i}", "link": f"https://github.com/org/repo{i}"} for i in range(10)]
        result = apply_domain_limits(articles, max_per_domain=3)
        self.assertEqual(len(result), 10)

    def test_exempts_reddit(self):
        articles = [{"title": f"Post {i}", "link": f"https://reddit.com/r/sub/comments/{i}"} for i in range(10)]
        result = apply_domain_limits(articles, max_per_domain=3)
        self.assertEqual(len(result), 10)

    def test_exempt_domains_set(self):
        for d in ("x.com", "twitter.com", "github.com", "reddit.com"):
            self.assertIn(d, DOMAIN_LIMIT_EXEMPT)


class TestGroupByTopics(unittest.TestCase):
    def test_groups_correctly(self):
        """Test that articles are assigned to their highest-priority topic only."""
        articles = [
            {"title": "A", "topic": "ai-frontier"},
            {"title": "B", "topic": "ai-infra"},
            {"title": "C", "topic": "ai-frontier"},
        ]
        groups = group_by_topics(articles)

        self.assertEqual(len(groups["ai-frontier"]), 2)
        self.assertEqual(len(groups["ai-infra"]), 1)
        article_a = next(a for a in groups["ai-frontier"] if a["title"] == "A")
        self.assertEqual(article_a["topic"], "ai-frontier")

    def test_no_topics_goes_uncategorized(self):
        articles = [{"title": "A", "topic": ""}, {"title": "B"}]
        groups = group_by_topics(articles)
        self.assertIn("uncategorized", groups)
        
    def test_cross_topic_deduplication(self):
        """Test that duplicate titles across topics are removed."""
        articles = [
            {"title": "Same Article", "topic": "ai-frontier", "final_score": 10},
            {"title": "Same Article", "topic": "ai-frontier", "final_score": 8},
            {"title": "Different Article", "topic": "ai-infra", "final_score": 5},
        ]
        groups = group_by_topics(articles)
        
        # Should have only 2 articles total (1 in ai-frontier, 1 in ai-infra)
        total = sum(len(articles) for articles in groups.values())
        self.assertEqual(total, 2)
        
        self.assertEqual(len(groups["ai-frontier"]), 1)
        self.assertEqual(groups["ai-frontier"][0]["final_score"], 10)

    def test_topic_order_follows_final_score_desc(self):
        articles = [
            {"title": "A1", "topic": "ai-frontier", "source_type": "twitter", "final_score": 10, "link": "https://x.com/1"},
            {"title": "A2", "topic": "ai-frontier", "source_type": "twitter", "final_score": 9, "link": "https://x.com/2"},
            {"title": "B1", "topic": "ai-frontier", "source_type": "rss", "final_score": 8.8, "link": "https://example.com/1"},
        ]
        groups = group_by_topics(articles)
        ordered = groups["ai-frontier"]
        self.assertEqual([item["title"] for item in ordered], ["A1", "A2", "B1"])


class TestFixtureData(unittest.TestCase):
    """Validate fixture data structure."""

    def test_rss_fixture(self):
        data = load_fixture("rss")
        self.assertIn("sources", data)
        for s in data["sources"]:
            for a in s.get("articles", []):
                self.assertIn("title", a)
                self.assertIn("link", a)


class TestV2EXScoring(unittest.TestCase):
    def test_reply_score_thresholds(self):
        self.assertEqual(calculate_v2ex_replies_score(10), 0)
        self.assertEqual(calculate_v2ex_replies_score(25), 1)
        self.assertEqual(calculate_v2ex_replies_score(60), 2)
        self.assertEqual(calculate_v2ex_replies_score(120), 3)
        self.assertEqual(calculate_v2ex_replies_score(250), 5)


class TestHistoryPenalty(unittest.TestCase):
    def test_history_similarity_scores_but_keeps_article(self):
        articles = [
            {
                "title": "OpenAI releases GPT-5 model",
                "link": "https://example.com/openai-gpt5",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_priority": 3,
            }
        ]
        result = deduplicate_articles(articles, previous_titles=["OpenAI releases GPT5 model"])
        self.assertEqual(len(result), 1)
        self.assertLess(result[0]["_score_components"]["history_score"], 0)
        self.assertGreater(result[0]["similarity_debug"]["history_similarity"], 0.88)
        self.assertTrue(result[0]["similarity_debug"]["history_duplicate"])

    def test_twitter_fixture(self):
        data = load_fixture("twitter")
        for s in data["sources"]:
            for a in s.get("articles", []):
                self.assertIn("title", a)
                self.assertIn("link", a)

    def test_github_fixture(self):
        data = load_fixture("github")
        for s in data["sources"]:
            for a in s.get("articles", []):
                self.assertIn("title", a)
                self.assertIn("link", a)

    def test_reddit_fixture(self):
        data = load_fixture("reddit")
        for s in data["subreddits"]:
            for a in s.get("articles", []):
                self.assertIn("title", a)
                self.assertIn("link", a)

    def test_google_fixture(self):
        data = load_fixture("google")
        for t in data["topics"]:
            for a in t.get("articles", []):
                self.assertIn("title", a)
                self.assertIn("link", a)


class TestDateHandling(unittest.TestCase):
    def test_recency_score_accepts_naive_iso_datetime(self):
        naive_date = (
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=2)
        ).replace(microsecond=0).isoformat()
        score = calculate_recency_score({"date": naive_date})
        self.assertGreaterEqual(score, 1.0)


class TestOutputNaming(unittest.TestCase):
    def test_deduplicated_article_uses_new_debug_field_names(self):
        articles = [
            {
                "title": "OpenAI releases GPT-5 model",
                "link": "https://example.com/openai-gpt5",
                "topic": "ai-frontier",
                "source_type": "rss",
                "source_name": "Example RSS",
                "source_priority": 3,
            }
        ]
        result = deduplicate_articles(articles, previous_titles=["OpenAI releases GPT5 model"])
        article = result[0]
        self.assertIn("_score_components", article)
        self.assertIn("scoring_debug", article)
        self.assertIn("similarity_debug", article)
        self.assertIn("source_names", article)
        self.assertIn("source_name_count", article)
        self.assertIn("history_score", article["scoring_debug"]["final_score"]["components"])
        self.assertIn("cross_source_hot_score", article["scoring_debug"]["final_score"]["components"])
        self.assertIn("recency_score", article["scoring_debug"]["final_score"]["components"])
        self.assertIn("matched_source_type_count", article["similarity_debug"]["cross_source_hot"])

    def test_load_previous_hotspots_uses_timezone_safe_cutoff(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_dir = Path(tmpdir)
            dated_json_dir = archive_dir / "2026-03-27" / "json"
            dated_json_dir.mkdir(parents=True)
            recent_name = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
            payload = {
                "topics": [
                    {
                        "id": "technology",
                        "items": [
                            {
                                "title": "Recent Story",
                                "link": "https://example.com/recent",
                            }
                        ],
                    }
                ]
            }
            (dated_json_dir / f"daily-{recent_name}.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            titles = load_previous_hotspots(archive_dir, days=14)
            self.assertIn("Recent Story", titles)

    def test_load_previous_hotspots_only_reads_daily_json_in_json_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_dir = Path(tmpdir)
            json_dir = archive_dir / "2026-03-30" / "json"
            markdown_dir = archive_dir / "2026-03-30" / "markdown"
            json_dir.mkdir(parents=True)
            markdown_dir.mkdir(parents=True)

            daily_payload = {
                "topics": [{"items": [{"title": "Daily Story", "link": "https://example.com/daily"}]}]
            }
            nondaily_payload = {
                "topics": [{"items": [{"title": "Ignored Story", "link": "https://example.com/ignored"}]}]
            }
            (json_dir / "daily.json").write_text(json.dumps(daily_payload), encoding="utf-8")
            (json_dir / "merge-sources.json").write_text(json.dumps(nondaily_payload), encoding="utf-8")
            (markdown_dir / "daily.json").write_text(json.dumps(nondaily_payload), encoding="utf-8")

            titles = load_previous_hotspots(archive_dir, days=14)

            self.assertIn("Daily Story", titles)
            self.assertNotIn("Ignored Story", titles)


class TestIntegration(unittest.TestCase):
    """End-to-end merge with fixture data."""

    def _collect_all_articles(self):
        all_articles = []
        for name, key, sub_key in [
            ("rss", "sources", "articles"),
            ("twitter", "sources", "articles"),
            ("github", "sources", "articles"),
            ("reddit", "subreddits", "articles"),
        ]:
            data = load_fixture(name)
            for source in data.get(key, []):
                for a in source.get(sub_key, []):
                    a["source_type"] = name
                    a.setdefault("topic", "")
                    all_articles.append(a)
        # Google has topics[].articles[]
        google = load_fixture("google")
        for topic in google.get("topics", []):
            for a in topic.get("articles", []):
                a["source_type"] = "google"
                a.setdefault("topic", topic.get("topic_id", ""))
                all_articles.append(a)
        return all_articles

    def test_merge_pipeline(self):
        articles = self._collect_all_articles()
        self.assertGreater(len(articles), 10)

        deduped = deduplicate_articles(articles)
        self.assertGreater(len(deduped), 0)
        self.assertLessEqual(len(deduped), len(articles))

        groups = group_by_topics(deduped)
        self.assertGreater(len(groups), 0)

        for topic, topic_articles in groups.items():
            limited = apply_domain_limits(topic_articles)
            # Twitter/GitHub/Reddit should NOT be limited
            for src in ("twitter", "github", "reddit"):
                before = sum(1 for a in topic_articles if a.get("source_type") == src)
                after = sum(1 for a in limited if a.get("source_type") == src)
                self.assertEqual(before, after,
                    f"{src} articles should not be limited in {topic}")

    def test_group_by_source_types_sorts_each_group_by_final_score(self):
        grouped = group_by_source_types(
            [
                {"title": "A", "source_type": "twitter", "final_score": 9.0},
                {"title": "B", "source_type": "twitter", "final_score": 12.0},
                {"title": "C", "source_type": "rss", "final_score": 8.0},
            ]
        )

        self.assertEqual([item["title"] for item in grouped["twitter"]], ["B", "A"])
        self.assertEqual([item["title"] for item in grouped["rss"]], ["C"])


class TestMergedOutput(unittest.TestCase):
    """Validate merged output structure."""

    def test_structure(self):
        data = load_fixture("merged")
        self.assertIn("source_types", data)
        self.assertIn("input_sources", data)
        self.assertIn("output_stats", data)
        self.assertIsInstance(data["source_types"], dict)
        self.assertNotIn("topics", data)

    def test_articles_have_scores(self):
        data = load_fixture("merged")
        for source_type, source_data in data["source_types"].items():
            self.assertIn("articles", source_data)
            scores = [article["final_score"] for article in source_data["articles"]]
            self.assertEqual(scores, sorted(scores, reverse=True), source_type)
            for article in source_data["articles"]:
                self.assertIn("final_score", article)


if __name__ == "__main__":
    unittest.main()
