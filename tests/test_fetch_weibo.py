#!/usr/bin/env python3
"""Tests for fetch-weibo.py."""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "fetch-weibo.py"

spec = importlib.util.spec_from_file_location("fetch_weibo", MODULE_PATH)
fetch_weibo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_weibo)


class TestFetchWeibo(unittest.TestCase):
    def test_transform_hot_item_uses_fixed_social_topic(self):
        article = fetch_weibo.transform_hot_item(
            {
                "note": "OpenAI 发布新模型后 Agent 开发门槛会继续降低吗",
                "num": "853 万",
                "label_name": "热",
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["topic"], "social")
        self.assertEqual(article["hot_score"], 8530000)
        self.assertIn("s.weibo.com/weibo?q=", article["link"])

    def test_transform_hot_item_uses_absolute_or_relative_link(self):
        article = fetch_weibo.transform_hot_item(
            {
                "word": "英伟达新 GPU 对算力行业意味着什么",
                "scheme": "/weibo?q=%23英伟达新GPU%23",
                "num": 12345,
                "realpos": 7,
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["link"], "https://s.weibo.com/weibo?q=%23英伟达新GPU%23")
        self.assertEqual(article["topic"], "social")
        self.assertEqual(article["rank"], 7)

    def test_transform_hot_item_keeps_non_ai_items_with_social_topic(self):
        article = fetch_weibo.transform_hot_item(
            {
                "word": "今晚吃什么家常菜",
                "num": 1000,
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["topic"], "social")

    def test_fetch_weibo_hot_uses_bb_browser_command(self):
        with patch.object(fetch_weibo, "run_bb_browser_site", return_value={"items": []}) as run_mock:
            data = fetch_weibo.fetch_weibo_hot(MagicMock(), limit=18)

        run_mock.assert_called_once_with(["weibo/hot", "18"])
        self.assertEqual(data["source_type"], "weibo")
        self.assertEqual(len(data["sources"][0]["request_traces"]), 1)
        self.assertEqual(data["sources"][0]["request_traces"][0]["status"], "ok")

    def test_run_bb_browser_site_parses_json_and_updates_cooldown(self):
        completed = MagicMock(returncode=0, stdout='{"items": []}', stderr="")
        with patch.object(fetch_weibo.subprocess, "run", return_value=completed) as run_mock:
            with patch.object(fetch_weibo, "throttle_after_success") as throttle_mock:
                fetch_weibo._last_success_at = None
                payload = fetch_weibo.run_bb_browser_site(["weibo/hot", "5"])

        self.assertEqual(payload, {"items": []})
        run_mock.assert_called_once()
        throttle_mock.assert_called_once()
        self.assertIsNotNone(fetch_weibo._last_success_at)


if __name__ == "__main__":
    unittest.main()
