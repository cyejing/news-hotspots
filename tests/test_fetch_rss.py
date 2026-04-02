#!/usr/bin/env python3
"""Tests for fetch-rss.py parsing helpers."""

import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "fetch-rss.py"

spec = importlib.util.spec_from_file_location("fetch_rss", MODULE_PATH)
fetch_rss = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_rss)
DEFAULTS_DIR = Path(__file__).parent.parent / "config" / "defaults"


class TestFeedParsing(unittest.TestCase):
    def setUp(self):
        self.cutoff = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_apply_runtime_config_updates_defaults(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            overlay_path = Path(tmpdir) / "news-hotspots-runtime.json"
            overlay_path.write_text(
                json.dumps(
                    {
                        "fetch": {
                            "rss": {
                                "request_timeout_s": 44,
                                "max_workers": 3,
                                "max_articles_per_feed": 11,
                                "retry_count": 2,
                                "retry_delay_s": 1.5,
                                "cache_ttl_hours": 9,
                            }
                        },
                        "cache": {
                            "rss_cache_path": "/tmp/custom-rss-cache.json",
                        },
                    }
                ),
                encoding="utf-8",
            )

            fetch_rss.apply_runtime_config(DEFAULTS_DIR, Path(tmpdir))

        self.assertEqual(fetch_rss.TIMEOUT, 44)
        self.assertEqual(fetch_rss.MAX_WORKERS, 3)
        self.assertEqual(fetch_rss.MAX_ARTICLES_PER_FEED, 11)
        self.assertEqual(fetch_rss.RETRY_COUNT, 2)
        self.assertEqual(fetch_rss.RETRY_DELAY, 1.5)
        self.assertEqual(fetch_rss.RSS_CACHE_TTL_HOURS, 9)
        self.assertEqual(str(fetch_rss.RSS_CACHE_PATH), "/tmp/custom-rss-cache.json")

    def test_parse_rss_with_pubdate(self):
        content = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Example RSS</title>
            <item>
              <title><![CDATA[RSS Title]]></title>
              <link>/post-1</link>
              <pubDate>Fri, 27 Mar 2026 10:00:00 +0000</pubDate>
            </item>
          </channel>
        </rss>
        """
        articles = fetch_rss.parse_feed(content, self.cutoff, "https://example.com/feed.xml")
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["title"], "RSS Title")
        self.assertEqual(articles[0]["link"], "https://example.com/post-1")

    def test_parse_atom_with_href_link(self):
        content = """<?xml version="1.0" encoding="utf-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <title>Example Atom</title>
          <entry>
            <title>Atom Title</title>
            <link rel="alternate" href="/entry-1" />
            <updated>2026-03-27T12:00:00Z</updated>
          </entry>
        </feed>
        """
        articles = fetch_rss.parse_feed(content, self.cutoff, "https://example.com/atom.xml")
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["title"], "Atom Title")
        self.assertEqual(articles[0]["link"], "https://example.com/entry-1")

    def test_parse_rss_extracts_summary(self):
        content = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>RSS Title</title>
              <link>https://example.com/post-1</link>
              <description><![CDATA[<p>Feed summary body</p>]]></description>
              <pubDate>Fri, 27 Mar 2026 10:00:00 +0000</pubDate>
            </item>
          </channel>
        </rss>
        """
        articles = fetch_rss.parse_feed(content, self.cutoff, "https://example.com/feed.xml")
        self.assertEqual(articles[0]["summary"], "Feed summary body")

    def test_parse_rss_without_summary_keeps_empty_summary(self):
        content = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>RSS Title</title>
              <link>https://example.com/post-1</link>
              <pubDate>Fri, 27 Mar 2026 10:00:00 +0000</pubDate>
            </item>
          </channel>
        </rss>
        """
        articles = fetch_rss.parse_feed(content, self.cutoff, "https://example.com/feed.xml")
        self.assertEqual(articles[0]["summary"], "")

    def test_parse_rss_with_dc_date_namespace(self):
        content = """<?xml version="1.0" encoding="UTF-8"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:dc="http://purl.org/dc/elements/1.1/">
          <item>
            <title>DC Date Title</title>
            <link>https://example.com/dc-date</link>
            <dc:date>2026-03-27T09:30:00Z</dc:date>
          </item>
        </rdf:RDF>
        """
        articles = fetch_rss.parse_feed(content, self.cutoff, "https://example.com/rdf.xml")
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["title"], "DC Date Title")
        self.assertEqual(articles[0]["link"], "https://example.com/dc-date")

    def test_non_feed_is_rejected(self):
        self.assertFalse(fetch_rss.is_probably_feed("<html><body>blocked</body></html>", "text/html"))

    def test_fetch_feed_with_retry_records_request_traces(self):
        source = {
            "id": "example-rss",
            "name": "Example RSS",
            "url": "https://example.com/feed.xml",
            "topic": "technology",
            "priority": 3,
        }
        rss_content = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>RSS Title</title>
              <link>https://example.com/post-1</link>
              <pubDate>Fri, 27 Mar 2026 10:00:00 +0000</pubDate>
            </item>
          </channel>
        </rss>
        """

        class FakeResponse:
            def __init__(self, content: str):
                self._content = content
                self.headers = {}
                self.url = "https://example.com/feed.xml"

            def read(self):
                return self._content.encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch.object(fetch_rss, "fetch_with_redirects", return_value=FakeResponse(rss_content)):
            with patch.object(fetch_rss, "_rss_cache", {}):
                result = fetch_rss.fetch_feed_with_retry(source, self.cutoff)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["count"], 1)
        self.assertIn("elapsed_s", result)
        self.assertEqual(len(result["request_traces"]), 1)
        self.assertEqual(result["request_traces"][0]["status"], "ok")
        self.assertEqual(result["articles"][0]["source_name"], "Example RSS")

    def test_fetch_feed_with_retry_aggregates_retry_elapsed_and_attempts(self):
        source = {
            "id": "example-rss",
            "name": "Example RSS",
            "url": "https://example.com/feed.xml",
            "topic": "technology",
            "priority": 3,
        }
        rss_content = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel><item><title>RSS Title</title><link>https://example.com/post-1</link><pubDate>Fri, 27 Mar 2026 10:00:00 +0000</pubDate></item></channel></rss>
        """

        class FakeResponse:
            def __init__(self, content: str):
                self._content = content
                self.headers = {}
                self.url = "https://example.com/feed.xml"

            def read(self):
                return self._content.encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        monotonic_values = iter([10.0, 10.1, 10.6, 12.0, 12.0, 12.0])
        def fake_monotonic():
            return next(monotonic_values)

        with patch.object(fetch_rss, "fetch_with_redirects", side_effect=[Exception("boom"), FakeResponse(rss_content)]):
            with patch.object(fetch_rss, "is_retryable_rss_error", return_value=True):
                with patch.object(fetch_rss.time, "sleep", return_value=None):
                    with patch.object(fetch_rss.time, "monotonic", side_effect=fake_monotonic):
                        with patch.object(fetch_rss, "_rss_cache", {}):
                            result = fetch_rss.fetch_feed_with_retry(source, self.cutoff)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["request_traces"][0]["attempt"], 2)
        self.assertEqual(result["request_traces"][0]["timing_s"]["active"], 1.4)
        self.assertEqual(result["request_traces"][0]["timing_s"]["total"], 2.0)


if __name__ == "__main__":
    unittest.main()
