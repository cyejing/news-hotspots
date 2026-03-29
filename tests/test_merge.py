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
DOMAIN_LIMIT_EXEMPT = merge_mod.DOMAIN_LIMIT_EXEMPT
calculate_v2ex_replies_bonus = merge_mod.calculate_v2ex_replies_bonus
calculate_recency_bonus = merge_mod.calculate_recency_bonus
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
        self.assertGreater(sim, 0.85)

    def test_length_diff_shortcut(self):
        sim = calculate_title_similarity("Short", "This is a much much longer title")
        self.assertLess(sim, 0.5)

    def test_chinese_similar(self):
        sim = calculate_title_similarity("OpenAI 发布 GPT-5 新模型", "OpenAI发布GPT5新模型")
        self.assertGreater(sim, 0.85)

    def test_mixed_language_similar(self):
        sim = calculate_title_similarity("Claude 4 API 发布", "Anthropic Claude-4 API正式发布")
        self.assertGreater(sim, 0.75)


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
            {"title": "Article A", "link": "https://example.com/a?ref=rss", "topics": ["ai-models"]},
            {"title": "Article A from RSS", "link": "https://example.com/a?ref=twitter", "topics": ["ai-models"]},
            {"title": "Article B", "link": "https://example.com/b", "topics": ["ai-models"]},
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 2)

    def test_removes_title_duplicates(self):
        articles = [
            {"title": "OpenAI releases GPT-5", "link": "https://a.com/1", "topics": ["ai-models"]},
            {"title": "OpenAI releases GPT-5!", "link": "https://b.com/2", "topics": ["ai-models"]},
            {"title": "Completely different article", "link": "https://c.com/3", "topics": ["ai-models"]},
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 2)

    def test_keeps_different_articles(self):
        articles = [
            {"title": "Python 3.12 released", "link": "https://a.com/1", "topics": ["ai-models"]},
            {"title": "Rust 1.75 announced", "link": "https://b.com/2", "topics": ["ai-models"]},
            {"title": "Go 1.22 is out", "link": "https://c.com/3", "topics": ["ai-models"]},
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 3)

    def test_prefers_highest_final_score_in_duplicate_cluster(self):
        articles = [
            {
                "title": "OpenAI releases GPT-5",
                "link": "https://a.com/1",
                "topics": ["ai-models"],
                "source_type": "rss",
                "source_priority": 3,
            },
            {
                "title": "OpenAI releases GPT-5 model",
                "link": "https://b.com/2",
                "topics": ["ai-models"],
                "source_type": "twitter",
                "source_priority": 5,
                "metrics": {"like_count": 2000, "retweet_count": 800},
            },
        ]
        result = deduplicate_articles(articles)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_type"], "twitter")
        self.assertIn("score_breakdown", result[0])
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
            {"title": "A", "topics": ["ai-models", "ai-agents"]},
            {"title": "B", "topics": ["ai-ecosystem"]},
            {"title": "C", "topics": ["ai-models"]},
        ]
        groups = group_by_topics(articles)
        
        # Article A should ONLY be in 'ai-models' (higher priority), not 'ai-agents'
        # This is the fix: each article appears in only ONE topic
        self.assertEqual(len(groups["ai-models"]), 2)  # Articles A and C
        self.assertEqual(len(groups["ai-ecosystem"]), 1)  # Article B
        
        # Article A should have primary_topic='ai-models' and all_topics preserved
        article_a = next(a for a in groups["ai-models"] if a["title"] == "A")
        self.assertEqual(article_a["primary_topic"], "ai-models")
        self.assertEqual(article_a["all_topics"], ["ai-models", "ai-agents"])
        
        # ai-agents topic should NOT exist since all its articles went to ai-models
        self.assertNotIn("ai-agents", groups)

    def test_no_topics_goes_uncategorized(self):
        articles = [{"title": "A", "topics": []}, {"title": "B"}]
        groups = group_by_topics(articles)
        self.assertIn("uncategorized", groups)
        
    def test_cross_topic_deduplication(self):
        """Test that duplicate titles across topics are removed."""
        articles = [
            {"title": "Same Article", "topics": ["ai-models", "ai-agents"], "quality_score": 10},
            {"title": "Same Article", "topics": ["ai-agents"], "quality_score": 8},
            {"title": "Different Article", "topics": ["ai-ecosystem"], "quality_score": 5},
        ]
        groups = group_by_topics(articles)
        
        # Should have only 2 articles total (1 in ai-models, 1 in ai-ecosystem)
        total = sum(len(articles) for articles in groups.values())
        self.assertEqual(total, 2)
        
        # "Same Article" should be in ai-models with score 10
        self.assertEqual(len(groups["ai-models"]), 1)
        self.assertEqual(groups["ai-models"][0]["quality_score"], 10)

    def test_topic_diversity_rerank_prefers_different_sources_early(self):
        articles = [
            {"title": "A1", "topics": ["ai-models"], "source_type": "twitter", "final_score": 10, "quality_score": 10, "link": "https://x.com/1"},
            {"title": "A2", "topics": ["ai-models"], "source_type": "twitter", "final_score": 9, "quality_score": 9, "link": "https://x.com/2"},
            {"title": "B1", "topics": ["ai-models"], "source_type": "rss", "final_score": 8.8, "quality_score": 8.8, "link": "https://example.com/1"},
        ]
        groups = group_by_topics(articles)
        ordered = groups["ai-models"]
        self.assertEqual(ordered[0]["source_type"], "twitter")
        self.assertEqual(ordered[1]["source_type"], "rss")


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
    def test_reply_bonus_thresholds(self):
        self.assertEqual(calculate_v2ex_replies_bonus(10), 0)
        self.assertEqual(calculate_v2ex_replies_bonus(25), 1)
        self.assertEqual(calculate_v2ex_replies_bonus(60), 2)
        self.assertEqual(calculate_v2ex_replies_bonus(120), 3)
        self.assertEqual(calculate_v2ex_replies_bonus(250), 5)


class TestHistoryPenalty(unittest.TestCase):
    def test_history_similarity_penalizes_but_keeps_article(self):
        articles = [
            {
                "title": "OpenAI releases GPT-5 model",
                "link": "https://example.com/openai-gpt5",
                "topics": ["ai-models"],
                "source_type": "rss",
                "source_priority": 3,
            }
        ]
        result = deduplicate_articles(articles, previous_titles=["OpenAI releases GPT5 model"])
        self.assertEqual(len(result), 1)
        self.assertLess(result[0]["score_breakdown"]["history_penalty"], 0)
        self.assertGreater(result[0]["similarity_debug"]["best_history_similarity"], 0.88)

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
    def test_recency_bonus_accepts_naive_iso_datetime(self):
        naive_date = (
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=2)
        ).replace(microsecond=0).isoformat()
        bonus = calculate_recency_bonus({"date": naive_date})
        self.assertGreaterEqual(bonus, 1.0)

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
                    a.setdefault("topics", [])
                    all_articles.append(a)
        # Google has topics[].articles[]
        google = load_fixture("google")
        for topic in google.get("topics", []):
            for a in topic.get("articles", []):
                a["source_type"] = "google"
                a.setdefault("topics", [])
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


class TestMergedOutput(unittest.TestCase):
    """Validate merged output structure."""

    def test_structure(self):
        data = load_fixture("merged")
        self.assertIn("topics", data)
        self.assertIn("input_sources", data)
        self.assertIn("output_stats", data)
        self.assertIsInstance(data["topics"], dict)

    def test_articles_have_scores(self):
        data = load_fixture("merged")
        for topic, tdata in data["topics"].items():
            self.assertIn("articles", tdata)
            for a in tdata["articles"]:
                self.assertIn("quality_score", a)


if __name__ == "__main__":
    unittest.main()
