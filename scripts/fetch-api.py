#!/usr/bin/env python3
"""
Fetch news from API sources.

Configured API source mapping:
- weibo-api -> fetch_weibo
- wallstreetcn-api -> fetch_wallstreetcn
- tencent-api -> fetch_tencent
- hacker-news-api -> fetch_hacker_news

Usage:
    python3 fetch-api.py [--output FILE] [--verbose]
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
from typing import Dict, List, Any

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

DEFAULT_API_SOURCES = [
    {"id": "weibo-api", "name": "Weibo Hot Search", "topics": ["world-affairs"], "priority": 3},
    {"id": "wallstreetcn-api", "name": "Wall Street CN", "topics": ["markets-business"], "priority": 4},
    {"id": "tencent-api", "name": "Tencent News", "topics": ["world-affairs"], "priority": 3},
    {"id": "hacker-news-api", "name": "Hacker News API", "topics": ["technology", "developer-tools"], "priority": 4},
]


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


def http_get_json(url: str, headers: Dict[str, str] = None, timeout: int = TIMEOUT) -> Dict:
    """HTTP GET JSON response."""
    req_headers = {"User-Agent": UA}
    if headers:
        req_headers.update(headers)

    apply_host_cooldown(url)
    
    if HAS_REQUESTS:
        resp = requests.get(url, headers=req_headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    else:
        opener = build_opener(RedirectHandler308)
        req = Request(url, headers=req_headers)
        with opener.open(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))


def fetch_weibo(limit: int = 15) -> List[Dict[str, Any]]:
    """Fetch Weibo hot search via API."""
    articles = []
    try:
        headers = {"Referer": "https://weibo.com/"}
        data = http_get_json("https://weibo.com/ajax/side/hotSearch", headers=headers)
        
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
                "topics": ["world-affairs"],
                "heat": str(heat),
            })
    except Exception as e:
        logging.debug(f"Weibo fetch failed: {e}")
    
    return articles


def fetch_wallstreetcn(limit: int = 15) -> List[Dict[str, Any]]:
    """Fetch WallStreetCN news via API."""
    articles = []
    try:
        url = "https://api-one.wallstcn.com/apiv1/content/information-flow?channel=global-channel&accept=article&limit=30"
        data = http_get_json(url)
        
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
                    "topics": ["markets-business"],
                })
    except Exception as e:
        logging.debug(f"WallStreetCN fetch failed: {e}")
    
    return articles


def fetch_tencent(limit: int = 15) -> List[Dict[str, Any]]:
    """Fetch Tencent News via API."""
    articles = []
    try:
        headers = {"Referer": "https://news.qq.com/"}
        url = "https://i.news.qq.com/web_backend/v2/getTagInfo?tagId=aEWqxLtdgmQ%3D"
        data = http_get_json(url, headers=headers)
        
        for news in data.get('data', {}).get('tabs', [{}])[0].get('articleList', [])[:limit]:
            articles.append({
                "title": news.get('title', '')[:200],
                "link": news.get('url') or news.get('link_info', {}).get('url', ''),
                "date": news.get('pub_time', '') or news.get('publish_time', ''),
                "source_id": "tencent-api",
                "source_type": "api",
                "source_name": "Tencent News",
                "topics": ["world-affairs"],
            })
    except Exception as e:
        logging.debug(f"Tencent News fetch failed: {e}")
    
    return articles


def fetch_hacker_news(limit: int = 15) -> List[Dict[str, Any]]:
    """Fetch Hacker News best stories via official Firebase API."""
    articles = []
    try:
        story_ids = http_get_json(
            "https://hacker-news.firebaseio.com/v0/beststories.json"
        ) or []

        for story_id in story_ids[: max(limit * 3, limit)]:
            item = http_get_json(
                f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
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
                "topics": ["technology", "developer-tools"],
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


def load_api_sources() -> List[Dict[str, Any]]:
    """Return the built-in API source list."""
    api_sources = [source.copy() for source in DEFAULT_API_SOURCES]
    logging.info(f"Loaded {len(api_sources)} API sources")
    return api_sources


def fetch_source(source: Dict[str, Any], limit: int = 15) -> Dict[str, Any]:
    """Fetch a single API source."""
    source_id = source["id"]
    name = source.get("name", source_id)
    topics = source.get("topics", [])
    priority = normalize_priority(source.get("priority"))
    
    fetcher = API_SOURCE_FETCHERS.get(source_id)
    if not fetcher:
        return {
            "source_id": source_id,
            "source_type": "api",
            "name": name,
            "priority": priority,
            "topics": topics,
            "status": "error",
            "error": f"Unknown source: {source_id}",
            "items": 0,
            "count": 0,
            "articles": [],
        }
    
    try:
        articles = fetcher(limit)
        
        for article in articles:
            article["topics"] = topics[:]
        
        return {
            "source_id": source_id,
            "source_type": "api",
            "name": name,
            "priority": priority,
            "topics": topics,
            "status": "ok",
            "items": len(articles),
            "count": len(articles),
            "articles": articles,
        }
    except Exception as e:
        return {
            "source_id": source_id,
            "source_type": "api",
            "name": name,
            "priority": priority,
            "topics": topics,
            "status": "error",
            "error": str(e)[:100],
            "items": 0,
            "count": 0,
            "articles": [],
        }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch news from API sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=15, help="Max items per source (default: 15)")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Time window in hours (ignored for API sources - they fetch real-time hot items)")
    parser.add_argument("--force", action="store_true", help="Force re-fetch (ignored for API sources)")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.verbose)

    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-api-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        sources = load_api_sources()

        logger.info(f"Fetching {len(sources)} API sources")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(fetch_source, source, args.limit): source for source in sources}
            
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
        
        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "api",
            "calls_total": len(results),
            "calls_ok": ok_count,
            "calls_kind": "sources",
            "items_total": total_articles,
            "sources_total": len(results),
            "sources_ok": ok_count,
            "total_articles": total_articles,
            "sources": results,
        }
        
        with open(args.output, "w", encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ Done: {ok_count}/{len(results)} API sources ok, {total_articles} articles → {args.output}")
        
        return 0
        
    except Exception as e:
        logger.error(f"💥 API fetch failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
