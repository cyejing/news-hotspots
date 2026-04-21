#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / "scripts" / "fetch-twitter.py"


def load_module():
    spec = importlib.util.spec_from_file_location("fetch_twitter", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch_twitter = load_module()


class TestFetchTwitter(unittest.TestCase):
    def recent_created_at(self) -> str:
        recent = fetch_twitter.local_now() - timedelta(hours=1)
        return recent.astimezone().strftime(fetch_twitter.TWITTER_DATE_FORMAT)

    def test_build_twitter_query_quotes_multi_word_excludes(self):
        compiled = fetch_twitter.build_twitter_query("AI agents", ["travel agent", "course"])

        self.assertEqual(compiled, 'AI agents -"travel agent" -course')

    def test_fetch_topic_does_not_append_exclude_terms(self):
        topic = {
            "id": "ai",
            "search": {
                "twitter_queries": ["(assistant OR agents)"],
                "exclude": ["tutorial", "travel agent"],
            },
        }
        payload = {
            "tweets": [
                {
                    "text": "assistant launch",
                    "url": "https://x.com/test/status/1",
                    "created_at": self.recent_created_at(),
                }
            ]
        }

        with patch.object(fetch_twitter, "run_bb_browser_site", return_value=payload):
            result = fetch_twitter.fetch_topic(topic, fetch_twitter.local_now() - timedelta(hours=24), fetch_twitter.logging.getLogger("test"))

        self.assertEqual(result["request_traces"][0]["target"], "(assistant OR agents)")
        self.assertEqual(result["articles"][0]["twitter_query"], "(assistant OR agents)")

    def test_apply_runtime_config_updates_count_and_results(self):
        original_timeout = fetch_twitter.DEFAULT_TIMEOUT
        original_count = fetch_twitter.DEFAULT_COUNT
        original_results = fetch_twitter.RESULTS_PER_QUERY
        original_cooldown = fetch_twitter.COOLDOWN_SECONDS
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                config_dir = Path(tmpdir)
                (config_dir / "news-hotspots-runtime.json").write_text(
                    json.dumps({"fetch": {"twitter": {"request_timeout_s": 61, "cooldown_s": 4, "count": 12, "results_per_query": 6}}}),
                    encoding="utf-8",
                )
                fetch_twitter.apply_runtime_config(ROOT / "config" / "defaults", config_dir)
            self.assertEqual(fetch_twitter.DEFAULT_TIMEOUT, 61)
            self.assertEqual(fetch_twitter.DEFAULT_COUNT, 12)
            self.assertEqual(fetch_twitter.RESULTS_PER_QUERY, 6)
            self.assertEqual(fetch_twitter.COOLDOWN_SECONDS, 4)
        finally:
            fetch_twitter.DEFAULT_TIMEOUT = original_timeout
            fetch_twitter.DEFAULT_COUNT = original_count
            fetch_twitter.RESULTS_PER_QUERY = original_results
            fetch_twitter.COOLDOWN_SECONDS = original_cooldown

    def test_load_sources_uses_split_twitter_config(self):
        sources = fetch_twitter.load_sources(ROOT / "config" / "defaults", None)
        self.assertTrue(sources)
        self.assertEqual(sources[0]["type"], "twitter")

    def test_parse_tweet_leaves_summary_empty_without_distinct_field(self):
        cutoff = fetch_twitter.local_now() - timedelta(hours=24)
        article = fetch_twitter.parse_tweet(
            {
                "text": "same as title",
                "url": "https://x.com/test/status/1",
                "created_at": self.recent_created_at(),
            },
            "ai",
            cutoff,
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["title"], "same as title")
        self.assertEqual(article["summary"], "")
        self.assertEqual(article["source_name"], "Twitter")
        self.assertEqual(datetime.fromisoformat(article["date"]).tzinfo, fetch_twitter.local_now().tzinfo)

    def test_parse_tweet_uses_distinct_summary_field(self):
        cutoff = fetch_twitter.local_now() - timedelta(hours=24)
        article = fetch_twitter.parse_tweet(
            {
                "text": "tweet body",
                "summary": "tweet summary",
                "url": "https://x.com/test/status/2",
                "created_at": self.recent_created_at(),
            },
            "ai",
            cutoff,
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["summary"], "tweet summary")
        self.assertNotEqual(article["summary"], article["title"])

    def test_parse_tweet_keeps_given_source_name(self):
        cutoff = fetch_twitter.local_now() - timedelta(hours=24)
        article = fetch_twitter.parse_tweet(
            {
                "text": "tweet body",
                "url": "https://x.com/test/status/2",
                "created_at": self.recent_created_at(),
            },
            "ai",
            cutoff,
            source_name="OpenAI",
        )

        self.assertEqual(article["source_name"], "OpenAI")

    def test_run_bb_browser_site_records_timeout_elapsed(self):
        with patch.object(fetch_twitter, "throttle_after_success", return_value=None), patch.object(
            fetch_twitter.subprocess,
            "run",
            side_effect=fetch_twitter.subprocess.TimeoutExpired(cmd=["bb-browser"], timeout=20),
        ):
            with self.assertRaises(fetch_twitter.TimedRuntimeError) as ctx:
                fetch_twitter.run_bb_browser_site(["twitter/tweets", "sama", "20"], timeout=20)

        self.assertEqual(ctx.exception.status, "timeout")
        self.assertIn("timed out after 20 seconds", str(ctx.exception))
        self.assertGreaterEqual(ctx.exception.elapsed_s, 0.0)

    def test_fetch_source_preserves_timeout_status_and_elapsed_in_trace(self):
        source = {"id": "openai", "handle": "OpenAI", "name": "OpenAI", "topic": "ai-frontier"}
        cutoff = fetch_twitter.local_now() - timedelta(hours=24)

        def raise_timeout(*args, **kwargs):
            raise fetch_twitter.TimedRuntimeError("timed out after 20 seconds", 20.0, status="timeout")

        with patch.object(fetch_twitter, "fetch_timeline", side_effect=raise_timeout):
            result = fetch_twitter.fetch_source(source, cutoff)

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["request_traces"][0]["status"], "timeout")
        self.assertEqual(result["request_traces"][0]["timing_s"]["active"], 20.0)
        self.assertEqual(result["request_traces"][0]["timing_s"]["total"], 20.0)

    def test_fetch_topic_preserves_timeout_status_and_elapsed_in_trace(self):
        topic = {"id": "ai", "search": {"twitter_queries": ["openai"]}}
        cutoff = fetch_twitter.local_now() - timedelta(hours=24)

        def raise_timeout(*args, **kwargs):
            raise fetch_twitter.TimedRuntimeError("timed out after 20 seconds", 20.0, status="timeout")

        with patch.object(fetch_twitter, "run_bb_browser_site", side_effect=raise_timeout):
            result = fetch_twitter.fetch_topic(topic, cutoff, fetch_twitter.logging.getLogger("test"))

        self.assertEqual(result["request_traces"][0]["status"], "timeout")
        self.assertEqual(result["request_traces"][0]["timing_s"]["active"], 20.0)
        self.assertEqual(result["request_traces"][0]["timing_s"]["total"], 20.0)


if __name__ == "__main__":
    unittest.main()
