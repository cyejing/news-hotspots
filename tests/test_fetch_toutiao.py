#!/usr/bin/env python3
"""Tests for fetch-toutiao.py."""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "fetch-toutiao.py"

spec = importlib.util.spec_from_file_location("fetch_toutiao", MODULE_PATH)
fetch_toutiao = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_toutiao)


class TestFetchToutiao(unittest.TestCase):
    def test_transform_hot_item_uses_fixed_social_topic(self):
        article = fetch_toutiao.transform_hot_item(
            {
                "title": "OpenAI 新模型推动 AI Agent 应用继续升温",
                "hot": "632 万",
                "label": "热",
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["topic"], "social")
        self.assertEqual(article["hot_score"], 6320000)
        self.assertIn("www.toutiao.com/search/", article["link"])

    def test_transform_hot_item_uses_relative_link_and_ai_infra_topic(self):
        article = fetch_toutiao.transform_hot_item(
            {
                "word": "英伟达 GPU 和算力基础设施竞争升级",
                "link": "/article/123456/",
                "score": 12345,
                "position": 9,
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["link"], "https://www.toutiao.com/article/123456/")
        self.assertEqual(article["topic"], "social")
        self.assertEqual(article["rank"], 9)

    def test_transform_hot_item_keeps_non_ai_items_with_social_topic(self):
        article = fetch_toutiao.transform_hot_item(
            {
                "query": "今晚吃什么夜宵",
                "score": 300,
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["topic"], "social")

    def test_fetch_toutiao_hot_uses_bb_browser_command(self):
        with patch.object(fetch_toutiao, "run_bb_browser_site", return_value={"items": []}) as run_mock:
            data = fetch_toutiao.fetch_toutiao_hot(MagicMock(), limit=16)

        run_mock.assert_called_once_with(["toutiao/hot", "16"])
        self.assertEqual(data["source_type"], "toutiao")
        self.assertEqual(len(data["sources"][0]["request_traces"]), 1)
        self.assertEqual(data["sources"][0]["request_traces"][0]["status"], "ok")

    def test_run_bb_browser_site_parses_json_and_updates_cooldown(self):
        completed = MagicMock(returncode=0, stdout='{"items": []}', stderr="")
        with patch.object(fetch_toutiao.subprocess, "run", return_value=completed) as run_mock:
            with patch.object(fetch_toutiao, "throttle_after_success") as throttle_mock:
                fetch_toutiao._last_success_at = None
                payload = fetch_toutiao.run_bb_browser_site(["toutiao/hot", "5"])

        self.assertEqual(payload, {"items": []})
        run_mock.assert_called_once()
        throttle_mock.assert_called_once()
        self.assertIsNotNone(fetch_toutiao._last_success_at)


if __name__ == "__main__":
    unittest.main()
