#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / "scripts" / "fetch-twitter.py"


def load_module():
    spec = importlib.util.spec_from_file_location("fetch_twitter", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch_twitter = load_module()


class TestFetchTwitter(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
