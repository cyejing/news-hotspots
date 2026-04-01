#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / "scripts" / "fetch-github.py"


def load_module():
    spec = importlib.util.spec_from_file_location("fetch_github", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch_github = load_module()


class TestFetchGitHub(unittest.TestCase):
    def test_apply_runtime_config_updates_defaults(self):
        original_timeout = fetch_github.TIMEOUT
        original_cooldown = fetch_github.GITHUB_COOLDOWN_DEFAULT
        original_limit = fetch_github.MAX_RELEASES_PER_REPO
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                config_dir = Path(tmpdir)
                (config_dir / "news-hotspots-runtime.json").write_text(
                    json.dumps({"fetch": {"github": {"request_timeout_s": 33, "cooldown_s": 4, "releases_per_repo": 7}}}),
                    encoding="utf-8",
                )
                fetch_github.apply_runtime_config(ROOT / "config" / "defaults", config_dir)
            self.assertEqual(fetch_github.TIMEOUT, 33)
            self.assertEqual(fetch_github.GITHUB_COOLDOWN_DEFAULT, 4)
            self.assertEqual(fetch_github.MAX_RELEASES_PER_REPO, 7)
        finally:
            fetch_github.TIMEOUT = original_timeout
            fetch_github.GITHUB_COOLDOWN_DEFAULT = original_cooldown
            fetch_github.MAX_RELEASES_PER_REPO = original_limit

    def test_load_sources_uses_split_github_config(self):
        sources = fetch_github.load_sources(ROOT / "config" / "defaults", None)
        self.assertTrue(sources)
        self.assertEqual(sources[0]["type"], "github")


if __name__ == "__main__":
    unittest.main()
