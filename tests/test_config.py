#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = ROOT / "scripts"
DEFAULTS_DIR = ROOT / "config" / "defaults"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


config_loader = load_module("config_loader", SCRIPTS_DIR / "config_loader.py")
validate_config = load_module("validate_config", SCRIPTS_DIR / "validate-config.py")


class TestConfigLoader(unittest.TestCase):
    def test_split_source_loaders_read_defaults(self):
        rss = config_loader.load_merged_rss_sources(DEFAULTS_DIR)
        twitter = config_loader.load_merged_twitter_sources(DEFAULTS_DIR)
        github = config_loader.load_merged_github_sources(DEFAULTS_DIR)
        reddit = config_loader.load_merged_reddit_sources(DEFAULTS_DIR)

        self.assertTrue(rss)
        self.assertTrue(twitter)
        self.assertTrue(github)
        self.assertTrue(reddit)
        self.assertEqual(rss[0]["type"], "rss")
        self.assertEqual(twitter[0]["type"], "twitter")
        self.assertEqual(github[0]["type"], "github")
        self.assertEqual(reddit[0]["type"], "reddit")

    def test_runtime_overlay_deep_merge(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            overlay = Path(tmpdir) / "news-hotspots-runtime.json"
            overlay.write_text(
                json.dumps({"fetch": {"google": {"results_per_query": 7}}}),
                encoding="utf-8",
            )
            runtime = config_loader.load_merged_runtime_config(DEFAULTS_DIR, Path(tmpdir))

        self.assertEqual(runtime["fetch"]["google"]["results_per_query"], 7)
        self.assertIn("cooldown_s", runtime["fetch"]["google"])

    def test_source_overlay_merges_by_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            overlay = Path(tmpdir) / "news-hotspots-rss.json"
            overlay.write_text(
                json.dumps(
                    {
                        "sources": [
                            {
                                "id": "openai-rss",
                                "type": "rss",
                                "name": "OpenAI Blog",
                                "url": "https://openai.com/news/rss.xml",
                                "enabled": False,
                                "priority": 4,
                                "topic": "ai-frontier",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            rss = config_loader.load_merged_rss_sources(DEFAULTS_DIR, Path(tmpdir))

        source = next(item for item in rss if item["id"] == "openai-rss")
        self.assertFalse(source["enabled"])


class TestValidateConfig(unittest.TestCase):
    def test_runtime_defaults_are_valid(self):
        runtime = json.loads((DEFAULTS_DIR / "runtime.json").read_text(encoding="utf-8"))
        self.assertEqual(validate_config.validate_runtime(runtime), [])

    def test_invalid_runtime_is_rejected(self):
        errors = validate_config.validate_runtime({"pipeline": {}, "fetch": {}, "diagnostics": {}, "cache": {}})
        self.assertTrue(errors)

    def test_invalid_source_topic_is_rejected(self):
        errors = validate_config.validate_source_file(
            {
                "sources": [
                    {
                        "id": "bad",
                        "type": "rss",
                        "name": "Bad",
                        "url": "https://example.com/feed.xml",
                        "enabled": True,
                        "priority": 3,
                        "topic": "missing-topic",
                    }
                ]
            },
            "rss",
            "url",
            ["ai-frontier"],
        )
        self.assertTrue(errors)

    def test_api_overlay_file_name_is_news_hotspots_api_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            overlay = Path(tmpdir) / "news-hotspots-api.json"
            overlay.write_text(
                json.dumps(
                    {
                        "sources": [
                            {
                                "id": "hacker-news-api",
                                "name": "Hacker News API",
                                "enabled": False,
                                "topic": "technology",
                                "priority": 3
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            merged = config_loader.load_merged_api_sources(DEFAULTS_DIR, Path(tmpdir))
        source = next(item for item in merged if item["id"] == "hacker-news-api")
        self.assertFalse(source["enabled"])


if __name__ == "__main__":
    unittest.main()
