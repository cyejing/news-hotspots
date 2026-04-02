#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

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

    def test_fetch_releases_with_retry_aggregates_retry_elapsed_and_attempts(self):
        source = {
            "id": "example-github",
            "name": "Example Repo",
            "repo": "example/repo",
            "topic": "technology",
            "priority": 3,
        }

        class FakeResponse:
            def __init__(self, payload: str):
                self._payload = payload
                self.headers = {}

            def read(self):
                return self._payload.encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        payload = json.dumps(
            [
                {
                    "tag_name": "v1.0.0",
                    "published_at": "2026-04-01T10:00:00Z",
                    "html_url": "https://github.com/example/repo/releases/tag/v1.0.0",
                    "body": "release body",
                }
            ]
        )
        monotonic_values = iter([20.0, 20.2, 20.8, 22.5, 22.5, 22.5])
        def fake_monotonic():
            return next(monotonic_values)

        with patch.object(fetch_github, "urlopen", side_effect=[Exception("boom"), FakeResponse(payload)]):
            with patch.object(fetch_github, "is_retryable_github_error", return_value=True):
                with patch.object(fetch_github.time, "sleep", return_value=None):
                    with patch.object(fetch_github.time, "monotonic", side_effect=fake_monotonic):
                        with patch.object(fetch_github, "_github_cache", {}):
                            result = fetch_github.fetch_releases_with_retry(
                                source,
                                datetime(2026, 3, 1, tzinfo=timezone.utc),
                                github_token=None,
                                no_cache=True,
                            )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["request_traces"][0]["attempt"], 2)
        self.assertEqual(result["request_traces"][0]["timing_s"]["active"], 1.7)
        self.assertEqual(result["request_traces"][0]["timing_s"]["total"], 2.5)
        self.assertEqual(result["articles"][0]["source_name"], "Example Repo")


if __name__ == "__main__":
    unittest.main()
