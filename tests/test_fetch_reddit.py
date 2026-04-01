#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / "scripts" / "fetch-reddit.py"


def load_module():
    spec = importlib.util.spec_from_file_location("fetch_reddit", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch_reddit = load_module()


class TestFetchReddit(unittest.TestCase):
    def test_apply_runtime_config_updates_results_per_query(self):
        original_timeout = fetch_reddit.DEFAULT_TIMEOUT
        original_results = fetch_reddit.RESULTS_PER_QUERY
        original_cooldown = fetch_reddit.COOLDOWN_SECONDS
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                config_dir = Path(tmpdir)
                (config_dir / "news-hotspots-runtime.json").write_text(
                    json.dumps({"fetch": {"reddit": {"request_timeout_s": 41, "cooldown_s": 5, "results_per_query": 8}}}),
                    encoding="utf-8",
                )
                fetch_reddit.apply_runtime_config(ROOT / "config" / "defaults", config_dir)
            self.assertEqual(fetch_reddit.DEFAULT_TIMEOUT, 41)
            self.assertEqual(fetch_reddit.RESULTS_PER_QUERY, 8)
            self.assertEqual(fetch_reddit.COOLDOWN_SECONDS, 5)
        finally:
            fetch_reddit.DEFAULT_TIMEOUT = original_timeout
            fetch_reddit.RESULTS_PER_QUERY = original_results
            fetch_reddit.COOLDOWN_SECONDS = original_cooldown

    def test_load_sources_uses_split_reddit_config(self):
        sources = fetch_reddit.load_sources(ROOT / "config" / "defaults", None)
        self.assertTrue(sources)
        self.assertEqual(sources[0]["type"], "reddit")


if __name__ == "__main__":
    unittest.main()
