#!/usr/bin/env python3
"""
API source_type 抓取脚本。

职责：
- 读取 `api.json` 中启用的 API 源配置
- 调用各 API 站点接口抓取数据
- 将成功结果标准化为统一 `articles`
- 将失败请求和耗时写入同名 `*.meta.json`

执行逻辑：
1. 加载 runtime 与 api source 配置
2. 并发请求不同 API source
3. 成功请求立即标准化为 article；失败请求只记录错误，不阻断其他请求
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只表达“抓到了什么数据”，顶层只保留 `generated`、`source_type`、`articles`
- `<step>.meta.json`
  只表达“这个 step 跑得怎么样”，记录总耗时、抓取个数、失败个数、失败明细和慢请求统计
"""

import json
import os
import argparse
import logging
import tempfile
import threading
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, build_opener, HTTPRedirectHandler
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, List, Any, Optional

try:
    from config_loader import load_merged_api_sources, load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_api_sources, load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

TIMEOUT = 30
MAX_WORKERS = 6
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
HOST_COOLDOWNS = {
    "hacker-news.firebaseio.com": 0.15,
}
_HOST_LAST_REQUEST_AT: Dict[str, float] = {}
_HOST_COOLDOWN_LOCK = threading.Lock()

def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global TIMEOUT, MAX_WORKERS, HOST_COOLDOWNS
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("api", {})
    diagnostics_config = runtime.get("diagnostics", {})
    TIMEOUT = int(fetch_config.get("request_timeout_s", TIMEOUT) or TIMEOUT)
    MAX_WORKERS = int(fetch_config.get("max_workers", MAX_WORKERS) or MAX_WORKERS)
    host_cooldowns = fetch_config.get("host_cooldowns", {})
    if isinstance(host_cooldowns, dict):
        HOST_COOLDOWNS = {str(host): float(value) for host, value in host_cooldowns.items()}
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


class RedirectHandler308(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if code in (301, 302, 303, 307, 308):
            newurl = newurl.replace(' ', '%20')
            new_headers = dict(req.headers)
            return Request(newurl, headers=new_headers, method=req.get_method())
        return None


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def normalize_priority(priority: Any, default: int = 3) -> int:
    """Normalize source priority into a 1-10 score."""
    if isinstance(priority, bool):
        return 8 if priority else default
    try:
        value = int(priority)
    except (TypeError, ValueError):
        return default
    return max(1, min(10, value))


def apply_host_cooldown(url: str) -> None:
    host = urlparse(url).netloc.lower()
    cooldown = HOST_COOLDOWNS.get(host, 0.0)
    if cooldown <= 0:
        return

    while True:
        wait_time = 0.0
        with _HOST_COOLDOWN_LOCK:
            last_request_at = _HOST_LAST_REQUEST_AT.get(host, 0.0)
            now = time.monotonic()
            elapsed = now - last_request_at
            if elapsed >= cooldown:
                _HOST_LAST_REQUEST_AT[host] = now
                return
            wait_time = cooldown - elapsed
        time.sleep(wait_time)


def http_get_json(
    url: str,
    headers: Dict[str, str] = None,
    timeout: Optional[int] = None,
    request_log: Optional[List[Dict[str, Any]]] = None,
) -> Dict:
    """HTTP GET JSON response."""
    req_headers = {"User-Agent": UA}
    if headers:
        req_headers.update(headers)
    effective_timeout = int(timeout if timeout is not None else TIMEOUT)

    apply_host_cooldown(url)

    started_at = time.monotonic()
    try:
        if HAS_REQUESTS:
            resp = requests.get(url, headers=req_headers, timeout=effective_timeout)
            resp.raise_for_status()
            payload = resp.json()
        else:
            opener = build_opener(RedirectHandler308)
            req = Request(url, headers=req_headers)
            with opener.open(req, timeout=effective_timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        if request_log is not None:
            request_log.append(build_request_trace(url, time.monotonic() - started_at, status="ok", method="GET"))
        return payload
    except Exception:
        if request_log is not None:
            request_log.append(build_request_trace(url, time.monotonic() - started_at, status="error", method="GET"))
        raise


def fetch_weibo(limit: int = 15, request_log: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Fetch Weibo hot search via API."""
    articles = []
    try:
        headers = {"Referer": "https://weibo.com/"}
        data = http_get_json("https://weibo.com/ajax/side/hotSearch", headers=headers, request_log=request_log)
        
        items = data.get('data', {}).get('realtime', [])
        
        for item in items[:limit]:
            title = item.get('note', '') or item.get('word', '')
            if not title:
                continue
            
            heat = item.get('num', 0)
            url = f"https://s.weibo.com/weibo?q={title}"
            
            articles.append({
                "title": title[:200],
                "link": url,
                "date": datetime.now(timezone.utc).isoformat(),
                "source_id": "weibo-api",
                "source_type": "api",
                "source_name": "Weibo Hot Search",
                "heat": str(heat),
            })
    except Exception as e:
        logging.debug(f"Weibo fetch failed: {e}")
    
    return articles


def fetch_wallstreetcn(limit: int = 15, request_log: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Fetch WallStreetCN news via API."""
    articles = []
    try:
        url = "https://api-one.wallstcn.com/apiv1/content/information-flow?channel=global-channel&accept=article&limit=30"
        data = http_get_json(url, request_log=request_log)
        
        for item in data.get('data', {}).get('items', [])[:limit]:
            res = item.get('resource')
            if res and (res.get('title') or res.get('content_short')):
                ts = res.get('display_time', 0)
                time_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M') if ts else ""
                
                articles.append({
                    "title": (res.get('title') or res.get('content_short', ''))[:200],
                    "link": res.get('uri', ''),
                    "date": time_str,
                    "source_id": "wallstreetcn-api",
                    "source_type": "api",
                    "source_name": "Wall Street CN",
                })
    except Exception as e:
        logging.debug(f"WallStreetCN fetch failed: {e}")
    
    return articles


def fetch_tencent(limit: int = 15, request_log: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Fetch Tencent News via API."""
    articles = []
    try:
        headers = {"Referer": "https://news.qq.com/"}
        url = "https://i.news.qq.com/web_backend/v2/getTagInfo?tagId=aEWqxLtdgmQ%3D"
        data = http_get_json(url, headers=headers, request_log=request_log)
        
        for news in data.get('data', {}).get('tabs', [{}])[0].get('articleList', [])[:limit]:
            articles.append({
                "title": news.get('title', '')[:200],
                "link": news.get('url') or news.get('link_info', {}).get('url', ''),
                "date": news.get('pub_time', '') or news.get('publish_time', ''),
                "source_id": "tencent-api",
                "source_type": "api",
                "source_name": "Tencent News",
            })
    except Exception as e:
        logging.debug(f"Tencent News fetch failed: {e}")
    
    return articles


def fetch_hacker_news(limit: int = 15, request_log: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Fetch Hacker News best stories via official Firebase API."""
    articles = []
    try:
        story_ids = http_get_json(
            "https://hacker-news.firebaseio.com/v0/beststories.json",
            request_log=request_log,
        ) or []

        for story_id in story_ids[: max(limit * 3, limit)]:
            item = http_get_json(
                f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json",
                request_log=request_log,
            )
            if not isinstance(item, dict):
                continue
            if item.get("type") != "story":
                continue

            title = (item.get("title") or "").strip()
            if not title:
                continue

            item_url = item.get("url") or f"https://news.ycombinator.com/item?id={story_id}"
            timestamp = item.get("time")
            date_iso = ""
            if isinstance(timestamp, (int, float)):
                date_iso = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

            articles.append({
                "title": title[:200],
                "link": item_url,
                "date": date_iso,
                "summary": (item.get("text") or "")[:500],
                "source_id": "hacker-news-api",
                "source_type": "api",
                "source_name": "Hacker News API",
                "hn_id": story_id,
                "score": int(item.get("score", 0) or 0),
                "comments": int(item.get("descendants", 0) or 0),
                "author": item.get("by", ""),
            })
            if len(articles) >= limit:
                break
    except Exception as e:
        logging.debug(f"Hacker News fetch failed: {e}")

    return articles


API_SOURCE_FETCHERS = {
    "weibo-api": fetch_weibo,
    "wallstreetcn-api": fetch_wallstreetcn,
    "tencent-api": fetch_tencent,
    "hacker-news-api": fetch_hacker_news,
}


def load_api_sources(defaults_dir: Optional[Path] = None, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Return configured API source list."""
    effective_defaults_dir = defaults_dir or Path("config/defaults")
    api_sources = [source.copy() for source in load_merged_api_sources(effective_defaults_dir, config_dir)]
    logging.info(f"Loaded {len(api_sources)} API sources")
    return api_sources


def fetch_source(source: Dict[str, Any], limit: int = 15) -> Dict[str, Any]:
    """Fetch a single API source."""
    source_id = source["id"]
    name = source.get("name", source_id)
    topic = str(source.get("topic") or "")
    priority = normalize_priority(source.get("priority"))
    
    fetcher = API_SOURCE_FETCHERS.get(source_id)
    request_log: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    if not fetcher:
        return {
            "source_id": source_id,
            "source_type": "api",
            "name": name,
            "priority": priority,
            "topic": topic,
            "status": "error",
            "error": f"Unknown source: {source_id}",
            "elapsed_s": round(time.monotonic() - started_at, 3),
            "items": 0,
            "count": 0,
            "articles": [],
            "request_traces": request_log,
            "failed_items": [normalize_failed_item(source_id, f"Unknown source: {source_id}", time.monotonic() - started_at)],
        }
    
    try:
        articles = fetcher(limit, request_log=request_log)
        
        for article in articles:
            article["topic"] = topic
        
        return {
            "source_id": source_id,
            "source_type": "api",
            "name": name,
            "priority": priority,
            "topic": topic,
            "status": "ok",
            "elapsed_s": round(time.monotonic() - started_at, 3),
            "items": len(articles),
            "count": len(articles),
            "articles": articles,
            "request_traces": request_log,
            "failed_items": [],
        }
    except Exception as e:
        return {
            "source_id": source_id,
            "source_type": "api",
            "name": name,
            "priority": priority,
            "topic": topic,
            "status": "error",
            "error": str(e)[:100],
            "elapsed_s": round(time.monotonic() - started_at, 3),
            "items": 0,
            "count": 0,
            "articles": [],
            "request_traces": request_log,
            "failed_items": [normalize_failed_item(source_id, str(e)[:100], time.monotonic() - started_at)],
        }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch news from API sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--limit", type=int, default=None, help="Max items per source")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Time window in hours (ignored for API sources - they fetch real-time hot items)")
    parser.add_argument("--force", action="store_true", help="Force re-fetch (ignored for API sources)")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    runtime = apply_runtime_config(args.defaults, effective_config_dir)

    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-api-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        default_limit = int(runtime.get("fetch", {}).get("api", {}).get("limit", 15) or 15)
        effective_limit = args.limit if args.limit is not None else default_limit
        sources = [
            source for source in load_api_sources(args.defaults, effective_config_dir)
            if source.get("enabled", True)
        ]

        logger.info(f"Fetching {len(sources)} API sources")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(fetch_source, source, effective_limit): source for source in sources}
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                if result["status"] == "ok":
                    logger.debug(f"✅ {result['name']}: {result['count']} articles")
                else:
                    logger.debug(f"❌ {result['name']}: {result.get('error', 'unknown error')}")
        
        results.sort(key=lambda x: (-normalize_priority(x.get("priority")), -x.get("count", 0)))
        
        ok_count = sum(1 for r in results if r["status"] == "ok")
        total_articles = sum(r.get("count", 0) for r in results)
        articles = [article for result in results for article in result.get("articles", []) if isinstance(article, dict)]
        failed_items = [
            normalize_failed_item(result.get("source_id"), result.get("error"), result.get("elapsed_s"))
            for result in results
            if result.get("status") != "ok"
        ]

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "api",
            "articles": articles,
        }
        request_traces = [trace for result in results for trace in result.get("request_traces", []) if isinstance(trace, dict)]
        meta = build_step_meta(
            step_key="api",
            status="ok" if ok_count == len(results) and total_articles > 0 else ("partial" if ok_count > 0 and total_articles > 0 else "error"),
            elapsed_s=sum(float(result.get("elapsed_s", 0) or 0) for result in results),
            items=total_articles,
            calls_total=len(results),
            calls_ok=ok_count,
            failed_items=failed_items,
            request_traces=request_traces,
        )
        write_result_with_meta(args.output, output, meta)
        
        logger.info(f"✅ Done: {ok_count}/{len(results)} API sources ok, {total_articles} articles → {args.output}")
        
        return 0 if total_articles > 0 else 1
        
    except Exception as e:
        logger.error(f"💥 API fetch failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
