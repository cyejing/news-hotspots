#!/usr/bin/env python3
"""Tests for fetch-github-trending.py."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "fetch-github-trending.py"
DEFAULTS_DIR = Path(__file__).parent.parent / "config" / "defaults"

spec = importlib.util.spec_from_file_location("fetch_github_trending", MODULE_PATH)
fetch_github_trending = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_github_trending)


class TestFetchGithubTrending(unittest.TestCase):
    def test_apply_runtime_config_updates_cooldown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            overlay_path = Path(tmpdir) / "news-hotspots-runtime.json"
            overlay_path.write_text(
                json.dumps({"fetch": {"github_trending": {"cooldown_s": 3.5}}}),
                encoding="utf-8",
            )

            fetch_github_trending.apply_runtime_config(DEFAULTS_DIR, Path(tmpdir))

        self.assertEqual(fetch_github_trending.get_github_trending_cooldown_seconds(), 3.5)

    def test_load_queries_only_reads_github_topic(self):
        queries = fetch_github_trending.load_github_trending_queries(DEFAULTS_DIR)
        self.assertEqual(len(queries), 5)
        self.assertTrue(all(query["topic"] == "github" for query in queries))
        self.assertTrue(all(" OR " not in query["q"] for query in queries))

    def test_load_queries_ignores_legacy_single_query(self):
        with patch.object(fetch_github_trending, "load_topics_config", return_value=[
            {
                "id": "github",
                "search": {
                    "queries": [],
                    "github_query": "legacy github query",
                },
            },
            {
                "id": "ai-frontier",
                "search": {
                    "queries": [],
                    "github_query": "ignored query",
                },
            },
        ]):
            queries = fetch_github_trending.load_github_trending_queries(DEFAULTS_DIR)

        self.assertEqual(queries, [])

    def test_trending_results_only_use_github_topic(self):
        payload = {
            "items": [
                {
                    "full_name": "example/project",
                    "name": "project",
                    "description": "Example repo",
                    "html_url": "https://github.com/example/project",
                    "stargazers_count": 1200,
                    "forks_count": 100,
                    "language": "Python",
                    "created_at": "2025-01-01T00:00:00Z",
                    "pushed_at": "2026-03-28T00:00:00Z",
                }
            ]
        }

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(payload).encode("utf-8")

        with patch.object(fetch_github_trending, "urlopen", return_value=FakeResponse()):
            with patch.object(fetch_github_trending.time, "sleep", return_value=None):
                result = fetch_github_trending.fetch_trending_repos(
                    hours=48,
                    github_token=None,
                    defaults_dir=DEFAULTS_DIR,
                    config_dir=None,
                )

        repos = result["repos"]
        self.assertEqual(len(repos), 1)
        self.assertEqual(repos[0]["topic"], "github")
        self.assertEqual(result["queries_total"], 5)
        self.assertEqual(result["queries_ok"], 5)

    def test_trending_returns_empty_when_no_queries_configured(self):
        with patch.object(fetch_github_trending, "load_github_trending_queries", return_value=[]):
            result = fetch_github_trending.fetch_trending_repos(
                hours=48,
                github_token=None,
                defaults_dir=DEFAULTS_DIR,
                config_dir=None,
            )

        self.assertEqual(result["repos"], [])
        self.assertEqual(result["queries_total"], 0)

    def test_trending_results_track_query_failures(self):
        with patch.object(
            fetch_github_trending,
            "load_github_trending_queries",
            return_value=[
                {"topic": "github", "q": "query one"},
                {"topic": "github", "q": "query two"},
            ],
        ):
            with patch.object(fetch_github_trending, "urlopen", side_effect=[Exception("boom"), Exception("boom")]):
                with patch.object(fetch_github_trending.time, "sleep", return_value=None):
                    result = fetch_github_trending.fetch_trending_repos(
                        hours=48,
                        github_token=None,
                        defaults_dir=DEFAULTS_DIR,
                        config_dir=None,
                    )

        self.assertEqual(result["queries_total"], 2)
        self.assertEqual(result["queries_ok"], 0)
        self.assertEqual(result["failed_items"][0]["id"], "query one")
        self.assertEqual(result["request_traces"][0]["status"], "error")


if __name__ == "__main__":
    unittest.main()
