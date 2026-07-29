#!/usr/bin/env python3
"""Tests for fetch-xueqiu.py."""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "fetch-xueqiu.py"

spec = importlib.util.spec_from_file_location("fetch_xueqiu", MODULE_PATH)
fetch_xueqiu = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_xueqiu)


class TestFetchXueqiu(unittest.TestCase):
    def test_transform_user_item_uses_value_investing_topic(self):
        article = fetch_xueqiu.transform_user_item(
            {
                "id": "123456",
                "text": "投资就是买生意，看懂现金流比看 K 线重要",
                "url": "https://xueqiu.com/6175873088/123456",
                "author": "大道无形我有型",
                "author_id": "6175873088",
                "created_at": "2026-08-07T08:00:00.000Z",
                "likes": 5280,
                "retweets": 12,
                "replies": 86,
            },
            fetch_xueqiu.DEFAULT_USER_NAME,
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["topic"], "value-investing")
        self.assertEqual(article["source_name"], fetch_xueqiu.SOURCE_NAME)
        self.assertEqual(article["author"], "大道无形我有型")
        self.assertEqual(article["likes"], 5280)
        self.assertIn("6175873088", article["link"])

    def test_transform_user_item_builds_link_from_ids_when_missing(self):
        article = fetch_xueqiu.transform_user_item(
            {
                "id": "987654",
                "text": "做对的事情，把事情做对",
                "author_id": "6175873088",
            },
            fetch_xueqiu.DEFAULT_USER_NAME,
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["link"], "https://xueqiu.com/6175873088/987654")
        self.assertEqual(article["topic"], "value-investing")

    def test_transform_user_item_returns_none_without_text(self):
        article = fetch_xueqiu.transform_user_item(
            {"id": "1", "url": "https://xueqiu.com/6175873088/1"},
            fetch_xueqiu.DEFAULT_USER_NAME,
        )
        self.assertIsNone(article)

    def test_transform_user_item_handles_millisecond_timestamp(self):
        article = fetch_xueqiu.transform_user_item(
            {
                "text": "市场短期是投票机，长期是称重机",
                "url": "https://xueqiu.com/6175873088/2",
                "created_at": 1723017600000,
            },
            fetch_xueqiu.DEFAULT_USER_NAME,
        )
        self.assertIsNotNone(article)
        self.assertTrue(article["date"].endswith("Z"))

    def test_fetch_xueqiu_user_uses_bb_browser_command_with_user_id(self):
        payload = {
            "user_id": "6175873088",
            "count": 1,
            "items": [
                {"id": "1", "text": "买股票就是买公司", "url": "https://xueqiu.com/6175873088/1"}
            ],
        }
        with patch.object(fetch_xueqiu, "run_bb_browser_site", return_value=payload) as run_mock:
            data = fetch_xueqiu.fetch_xueqiu_user(MagicMock(), limit=15)

        run_mock.assert_called_once_with(["xueqiu/user", fetch_xueqiu.DEFAULT_USER_ID, "15"])
        self.assertEqual(data["source_type"], "xueqiu")
        self.assertEqual(data["sources"][0]["topic"], "value-investing")
        self.assertEqual(data["sources"][0]["priority"], fetch_xueqiu.SOURCE_PRIORITY)
        self.assertEqual(len(data["sources"][0]["articles"]), 1)
        self.assertEqual(data["sources"][0]["articles"][0]["topic"], "value-investing")
        self.assertEqual(len(data["sources"][0]["request_traces"]), 1)
        self.assertEqual(data["sources"][0]["request_traces"][0]["status"], "ok")

    def test_fetch_xueqiu_user_raises_on_adapter_error(self):
        with patch.object(fetch_xueqiu, "run_bb_browser_site", return_value={"error": "HTTP 401"}):
            with self.assertRaises(fetch_xueqiu.TimedRuntimeError):
                fetch_xueqiu.fetch_xueqiu_user(MagicMock(), limit=10)

    def test_run_bb_browser_site_parses_json_and_updates_cooldown(self):
        completed = MagicMock(returncode=0, stdout='{"items": []}', stderr="")
        with patch.object(fetch_xueqiu.subprocess, "run", return_value=completed) as run_mock:
            with patch.object(fetch_xueqiu, "throttle_after_success") as throttle_mock:
                fetch_xueqiu._last_success_at = None
                payload = fetch_xueqiu.run_bb_browser_site(["xueqiu/user", "6175873088", "5"])

        self.assertEqual(payload, {"items": []})
        run_mock.assert_called_once()
        throttle_mock.assert_called_once()
        self.assertIsNotNone(fetch_xueqiu._last_success_at)


if __name__ == "__main__":
    unittest.main()
