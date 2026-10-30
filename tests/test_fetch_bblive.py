"""fetch-bblive 单元测试：transformer 契约 + 多源聚合 + 失败降级。"""

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "fetch-bblive.py"
spec = importlib.util.spec_from_file_location("fetch_bblive", SCRIPT)
fetch_bblive = importlib.util.module_from_spec(spec)
sys.modules.setdefault("fetch_bblive", fetch_bblive)
spec.loader.exec_module(fetch_bblive)


class TestTransformers(unittest.TestCase):
    def test_36kr_item(self):
        item = {
            "rank": 1,
            "id": "123",
            "title": "热门中概股美股盘前多数下跌",
            "description": "36氪获悉，百度跌超2%",
            "timestamp": "2026-10-30T13:00:43.835Z",
            "url": "https://36kr.com/newsflashes/123",
        }
        a = fetch_bblive.transform_36kr_item(item)
        self.assertEqual(a["title"], "热门中概股美股盘前多数下跌")
        self.assertEqual(a["link"], "https://36kr.com/newsflashes/123")
        self.assertEqual(a["topic"], "business")
        self.assertEqual(a["source_name"], "36氪快讯")
        self.assertTrue(a["date"].startswith("2026-10-30T21:00:43"))  # UTC→+08:00

    def test_36kr_item_missing_title_returns_none(self):
        self.assertIsNone(fetch_bblive.transform_36kr_item({"description": "x"}))

    def test_eastmoney_item(self):
        item = {
            "rank": 1,
            "title": "金融产品网络营销管理办法实施满月",
            "summary": "行业系统性整改全面铺开",
            "source": "证券日报",
            "time": "2026-10-31 02:14:00",
            "url": "https://finance.eastmoney.com/a/1.html",
        }
        a = fetch_bblive.transform_eastmoney_item(item)
        self.assertEqual(a["topic"], "business")
        self.assertEqual(a["date"], "2026-10-31T02:14:00")
        self.assertEqual(a["metrics"]["origin"], "证券日报")

    def test_bilibili_item(self):
        item = {"rank": 1, "keyword": "央视记者直击德国总统访华", "show_name": "央视记者直击德国总统访华", "is_hot": False, "icon": None, "search_url": "https://search.bilibili.com/all?keyword=x"}
        a = fetch_bblive.transform_bilibili_item(item)
        self.assertEqual(a["topic"], "social")
        self.assertEqual(a["title"], "央视记者直击德国总统访华")
        self.assertEqual(a["source_name"], "B站热搜")


class TestFetchBblive(unittest.TestCase):
    def test_aggregates_three_sources(self):
        payloads = {
            "36kr/newsflash": {"count": 1, "items": [{"rank": 1, "title": "t1", "url": "https://36kr.com/1", "timestamp": "2026-10-30T13:00:43.835Z"}]},
            "eastmoney/news": {"count": 1, "news": [{"rank": 1, "title": "t2", "time": "2026-10-31 02:14:00", "url": "https://e.cn/1"}]},
            "bilibili/trending": {"count": 1, "items": [{"rank": 1, "keyword": "t3", "search_url": "https://b.tv/1"}]},
        }

        def fake_run(command, timeout=None):
            adapter = command[0]
            return payloads[adapter]

        with patch.object(fetch_bblive, "run_bb_browser_site", side_effect=fake_run):
            data = fetch_bblive.fetch_bblive(fetch_bblive.logging.getLogger("t"), 20)

        self.assertEqual(data["calls_total"], 3)
        self.assertEqual(data["calls_ok"], 3)
        self.assertEqual(len(data["articles"]), 3)
        topics = {a["topic"] for a in data["articles"]}
        self.assertEqual(topics, {"business", "social"})

    def test_source_failure_degrades_not_crashes(self):
        def fake_run(command, timeout=None):
            if command[0] == "eastmoney/news":
                raise fetch_bblive.TimedRuntimeError("Daemon request timed out", 45.0)
            return {"count": 1, "items": [{"rank": 1, "keyword": "t", "title": "t", "url": "https://x/1"}]}

        with patch.object(fetch_bblive, "run_bb_browser_site", side_effect=fake_run):
            data = fetch_bblive.fetch_bblive(fetch_bblive.logging.getLogger("t"), 20)

        self.assertEqual(data["calls_total"], 3)
        self.assertEqual(data["calls_ok"], 2)
        statuses = [s["status"] for s in data["sources"]]
        self.assertEqual(sorted(statuses), ["error", "ok", "ok"])


if __name__ == "__main__":
    unittest.main()
