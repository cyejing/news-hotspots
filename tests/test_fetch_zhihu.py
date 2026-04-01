#!/usr/bin/env python3
"""Tests for fetch-zhihu.py."""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
MODULE_PATH = SCRIPTS_DIR / "fetch-zhihu.py"

spec = importlib.util.spec_from_file_location("fetch_zhihu", MODULE_PATH)
fetch_zhihu = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_zhihu)


class TestFetchZhihu(unittest.TestCase):
    def test_transform_hot_item_uses_fixed_social_topic(self):
        article = fetch_zhihu.transform_hot_item(
            {
                "title": "OpenAI 新模型和 Agent 体系到底强在哪？",
                "url": "https://www.zhihu.com/question/123",
                "excerpt": "讨论 OpenAI、ChatGPT 和 AI Agent 的最新进展",
                "hot": "1264 万热度",
                "answer_count": 321,
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["topic"], "social")
        self.assertEqual(article["hot_score"], 12640000)

    def test_transform_hot_item_builds_question_url_from_id(self):
        article = fetch_zhihu.transform_hot_item(
            {
                "questionId": 456,
                "target": {
                    "title": "英伟达新一代 GPU 会怎样影响算力市场？",
                    "excerpt": "聚焦 GPU、芯片和算力供给",
                },
                "metrics": {
                    "hot_score": 98765,
                    "answer_count": 18,
                },
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["link"], "https://www.zhihu.com/question/456")
        self.assertEqual(article["topic"], "social")

    def test_transform_hot_item_keeps_non_ai_items_with_social_topic(self):
        article = fetch_zhihu.transform_hot_item(
            {
                "title": "第一次自己在家做红烧肉需要注意什么？",
                "url": "https://www.zhihu.com/question/789",
                "excerpt": "家常菜经验交流",
            }
        )

        self.assertIsNotNone(article)
        self.assertEqual(article["topic"], "social")

    def test_fetch_zhihu_hot_uses_bb_browser_command(self):
        with patch.object(fetch_zhihu, "run_bb_browser_site", return_value={"items": []}) as run_mock:
            data = fetch_zhihu.fetch_zhihu_hot(MagicMock(), limit=12)

        run_mock.assert_called_once_with(["zhihu/hot", "12"])
        self.assertEqual(data["source_type"], "zhihu")
        self.assertEqual(len(data["sources"][0]["request_traces"]), 1)
        self.assertEqual(data["sources"][0]["request_traces"][0]["status"], "ok")

    def test_run_bb_browser_site_parses_json_and_updates_cooldown(self):
        completed = MagicMock(returncode=0, stdout='{"items": []}', stderr="")
        with patch.object(fetch_zhihu.subprocess, "run", return_value=completed) as run_mock:
            with patch.object(fetch_zhihu, "throttle_after_success") as throttle_mock:
                fetch_zhihu._last_success_at = None
                payload = fetch_zhihu.run_bb_browser_site(["zhihu/hot", "5"])

        self.assertEqual(payload, {"items": []})
        run_mock.assert_called_once()
        throttle_mock.assert_called_once()
        self.assertIsNotNone(fetch_zhihu._last_success_at)


if __name__ == "__main__":
    unittest.main()
