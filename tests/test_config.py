#!/usr/bin/env python3
"""Tests for config_loader.py."""

import importlib.util
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "config_loader.py"

spec = importlib.util.spec_from_file_location("config_loader", MODULE_PATH)
config_loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config_loader)

load_merged_sources = config_loader.load_merged_sources
load_merged_topics = config_loader.load_merged_topics

DEFAULTS_DIR = Path(__file__).parent.parent / "config" / "defaults"


def load_grouped_sources():
    with open(DEFAULTS_DIR / "sources.json", "r", encoding="utf-8") as f:
        return json.load(f)["sources"]


class TestLoadSources(unittest.TestCase):
    def test_loads_defaults(self):
        sources = load_merged_sources(DEFAULTS_DIR)
        self.assertGreater(len(sources), 100)

    def test_all_sources_have_required_fields(self):
        sources = load_merged_sources(DEFAULTS_DIR)
        for source in sources:
            self.assertIn("id", source, f"Source missing id: {source}")
            self.assertIn("type", source, f"Source missing type: {source}")
            self.assertIn("enabled", source, f"Source missing enabled: {source}")

    def test_source_types(self):
        sources = load_merged_sources(DEFAULTS_DIR)
        types = {source["type"] for source in sources}
        self.assertIn("rss", types)
        self.assertIn("twitter", types)
        self.assertIn("github", types)
        self.assertIn("reddit", types)

    def test_grouped_defaults_flatten(self):
        grouped = load_grouped_sources()
        sources = load_merged_sources(DEFAULTS_DIR)
        self.assertEqual(
            len(sources),
            sum(len(entries) for entries in grouped.values()),
        )

    def test_user_overlay_merges(self):
        """User overlay should override matching IDs and add new ones."""
        with tempfile.TemporaryDirectory() as tmpdir:
            overlay = {
                "sources": {
                    "rss": [
                        {
                            "id": "test-new-source",
                            "type": "rss",
                            "name": "Test RSS",
                            "enabled": True,
                            "priority": 3,
                            "topics": ["ai-models"],
                            "url": "https://test.com/feed",
                        }
                    ]
                }
            }
            overlay_path = Path(tmpdir) / "news-hotspots-sources.json"
            with open(overlay_path, "w", encoding="utf-8") as f:
                json.dump(overlay, f)

            sources = load_merged_sources(DEFAULTS_DIR, Path(tmpdir))
            ids = [source["id"] for source in sources]
            self.assertIn("test-new-source", ids)

    def test_user_overlay_disables(self):
        """User overlay with enabled=false should disable a default source."""
        defaults = load_merged_sources(DEFAULTS_DIR)
        first = defaults[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            overlay = {
                "sources": {
                    first["type"]: [
                        {"id": first["id"], "type": first["type"], "enabled": False}
                    ]
                }
            }
            overlay_path = Path(tmpdir) / "news-hotspots-sources.json"
            with open(overlay_path, "w", encoding="utf-8") as f:
                json.dump(overlay, f)

            sources = load_merged_sources(DEFAULTS_DIR, Path(tmpdir))
            matched = [source for source in sources if source["id"] == first["id"]]
            self.assertEqual(len(matched), 1)
            self.assertFalse(matched[0]["enabled"])

    def test_user_overlay_can_move_source_to_new_group(self):
        defaults = load_merged_sources(DEFAULTS_DIR)
        first = defaults[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            overlay = {
                "sources": {
                    "twitter": [
                        {
                            "id": first["id"],
                            "type": "twitter",
                            "name": first["name"],
                            "handle": "example_handle",
                            "enabled": True,
                            "priority": first.get("priority", 3),
                            "topics": first.get("topics", []),
                        }
                    ]
                }
            }
            overlay_path = Path(tmpdir) / "news-hotspots-sources.json"
            with open(overlay_path, "w", encoding="utf-8") as f:
                json.dump(overlay, f)

            sources = load_merged_sources(DEFAULTS_DIR, Path(tmpdir))
            moved = next(source for source in sources if source["id"] == first["id"])
            self.assertEqual(moved["type"], "twitter")
            self.assertEqual(moved["handle"], "example_handle")

    def test_no_overlay_dir(self):
        """Should work fine with no user config dir."""
        sources = load_merged_sources(DEFAULTS_DIR, None)
        self.assertGreater(len(sources), 100)


class TestLoadTopics(unittest.TestCase):
    def test_loads_defaults(self):
        topics = load_merged_topics(DEFAULTS_DIR)
        self.assertGreater(len(topics), 0)

    def test_topics_have_required_fields(self):
        topics = load_merged_topics(DEFAULTS_DIR)
        for topic in topics:
            self.assertIn("id", topic, f"Topic missing id: {topic}")
            self.assertIn("label", topic, f"Topic missing label: {topic}")

    def test_topic_ids(self):
        topics = load_merged_topics(DEFAULTS_DIR)
        ids = [topic["id"] for topic in topics]
        self.assertIn("ai-models", ids)
        self.assertIn("ai-agents", ids)
        self.assertIn("technology", ids)
        self.assertIn("github", ids)

    def test_github_sources_use_single_github_topic(self):
        sources = load_merged_sources(DEFAULTS_DIR)
        github_sources = [source for source in sources if source["type"] == "github"]
        self.assertTrue(github_sources)
        for source in github_sources:
            self.assertEqual(source.get("topics"), ["github"])


class TestSourceCounts(unittest.TestCase):
    """Verify loader counts stay in sync with grouped defaults."""

    @classmethod
    def setUpClass(cls):
        cls.sources = load_merged_sources(DEFAULTS_DIR)
        cls.raw_grouped = load_grouped_sources()
        cls.expected_by_type = {
            source_type: len(entries) for source_type, entries in cls.raw_grouped.items()
        }
        cls.actual_by_type = Counter(source["type"] for source in cls.sources)

    def test_total_sources(self):
        expected_total = sum(self.expected_by_type.values())
        self.assertEqual(len(self.sources), expected_total)

    def test_enabled_sources(self):
        enabled = [source for source in self.sources if source.get("enabled", True)]
        self.assertGreaterEqual(len(enabled), 120)

    def test_counts_match_grouped_config(self):
        for source_type, expected_count in self.expected_by_type.items():
            self.assertEqual(self.actual_by_type.get(source_type, 0), expected_count)

    def test_default_priorities_stay_compact(self):
        priorities = [source.get("priority", 3) for source in self.sources]
        self.assertTrue(priorities, "Expected at least one source priority")
        self.assertGreaterEqual(min(priorities), 3)
        self.assertLessEqual(max(priorities), 5)

    def test_default_priorities_are_majority_three(self):
        priorities = [source.get("priority", 3) for source in self.sources]
        three_ratio = priorities.count(3) / len(priorities)
        self.assertGreaterEqual(
            three_ratio,
            0.55,
            f"Expected most default priorities to stay at 3, got ratio={three_ratio:.3f}",
        )


if __name__ == "__main__":
    unittest.main()
