#!/usr/bin/env python3
"""
RSS source_type 抓取脚本。

职责：
- 读取 `rss.json` 中启用的 RSS 源配置
- 并发抓取 RSS feed，并在单 feed 内按重试策略处理失败
- 将 feed 内容标准化为统一 `articles`
- 将失败 feed、耗时与慢请求统计写入 `*.meta.json`

执行逻辑：
1. 加载 runtime 与 RSS source 配置
2. 使用线程池并发拉取 feed
3. 每个 feed 成功后立即解析并标准化为 article
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存标准化后的 RSS article，供 `merge-sources.py` 合并
- `<step>.meta.json`
  只保存抓取诊断，包括失败请求与耗时信息
"""

import json
import re
import sys
import os
import argparse
import logging
import time
import tempfile
from html import unescape
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen, Request, build_opener, HTTPRedirectHandler
from urllib.error import URLError, HTTPError
from urllib.parse import urljoin, urlparse
from pathlib import Path
from typing import Dict, List, Any, Optional
import threading
from xml.etree import ElementTree as ET
from email.utils import parsedate_to_datetime

try:
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta


def normalize_priority(priority: Any, default: int = 3) -> int:
    """Normalize source priority into a 1-10 score."""
    if isinstance(priority, bool):
        return 8 if priority else default
    try:
        value = int(priority)
    except (TypeError, ValueError):
        return default
    return max(1, min(10, value))

# Try to import feedparser, fall back to XML parsing
try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False
    logging.warning("feedparser not installed — using XML fallback parser. Install with: pip install feedparser")

TIMEOUT = 30
MAX_WORKERS = 10  
MAX_ARTICLES_PER_FEED = 20
RETRY_COUNT = 1
RETRY_DELAY = 2.0  # seconds
RSS_CACHE_PATH = "/tmp/news-hotspots-rss-cache.json"
RSS_CACHE_TTL_HOURS = 24


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global TIMEOUT, MAX_WORKERS, MAX_ARTICLES_PER_FEED, RETRY_COUNT, RETRY_DELAY, RSS_CACHE_PATH, RSS_CACHE_TTL_HOURS
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("rss", {})
    diagnostics_config = runtime.get("diagnostics", {})
    cache_config = runtime.get("cache", {})
    TIMEOUT = int(fetch_config.get("request_timeout_s", TIMEOUT) or TIMEOUT)
    MAX_WORKERS = int(fetch_config.get("max_workers", MAX_WORKERS) or MAX_WORKERS)
    MAX_ARTICLES_PER_FEED = int(fetch_config.get("max_articles_per_feed", MAX_ARTICLES_PER_FEED) or MAX_ARTICLES_PER_FEED)
    RETRY_COUNT = int(fetch_config.get("retry_count", RETRY_COUNT) or RETRY_COUNT)
    RETRY_DELAY = float(fetch_config.get("retry_delay_s", RETRY_DELAY) or RETRY_DELAY)
    RSS_CACHE_TTL_HOURS = int(fetch_config.get("cache_ttl_hours", RSS_CACHE_TTL_HOURS) or RSS_CACHE_TTL_HOURS)
    RSS_CACHE_PATH = str(cache_config.get("rss_cache_path", RSS_CACHE_PATH) or RSS_CACHE_PATH)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


class RedirectHandler308(HTTPRedirectHandler):
    """Custom redirect handler that also handles 308 Permanent Redirect."""
    
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if code in (301, 302, 303, 307, 308):
            newurl = newurl.replace(' ', '%20')
            new_headers = dict(req.headers)
            return Request(newurl, headers=new_headers, method=req.get_method())
        return None


def fetch_with_redirects(url, headers, timeout=None):
    """Fetch URL with support for 308 redirects."""
    opener = build_opener(RedirectHandler308)
    req = Request(url, headers=headers)
    effective_timeout = int(timeout if timeout is not None else TIMEOUT)
    return opener.open(req, timeout=effective_timeout)


def is_retryable_rss_error(exc: Exception) -> bool:
    """Retry only transient RSS failures; fail fast on stable bad feeds."""
    if isinstance(exc, HTTPError):
        return exc.code in {408, 425, 429, 500, 502, 503, 504}
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, URLError):
        reason = str(getattr(exc, "reason", exc)).lower()
        transient_markers = ("timed out", "timeout", "tempor", "connection reset", "connection aborted")
        return any(marker in reason for marker in transient_markers)
    message = str(exc).lower()
    transient_markers = ("timed out", "timeout", "tempor", "connection reset", "connection aborted")
    return any(marker in message for marker in transient_markers)


def setup_logging(verbose: bool) -> logging.Logger:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def parse_date_regex(s: str) -> Optional[datetime]:
    """Parse date string using regex patterns (fallback method)."""
    if not s:
        return None
    s = s.strip()

    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError, IndexError, OverflowError):
        pass
    
    # Common date formats
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z", 
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
            
    # ISO fallback
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt
    except (ValueError, AttributeError):
        pass
        
    return None


def extract_cdata(text: str) -> str:
    """Extract content from CDATA sections."""
    m = re.search(r"<!\[CDATA\[(.*?)\]\]>", text, re.DOTALL)
    return m.group(1) if m else text


def strip_tags(html: str) -> str:
    """Remove HTML tags from text."""
    return unescape(re.sub(r"<[^>]+>", "", html)).strip()


def _xml_local_name(tag: str) -> str:
    """Return the local element name without namespace."""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    if ":" in tag:
        return tag.rsplit(":", 1)[-1]
    return tag


def _xml_child_elements(parent: ET.Element, local_name: str) -> List[ET.Element]:
    """Return direct child elements matching a local tag name."""
    return [child for child in list(parent) if _xml_local_name(child.tag) == local_name]


def _xml_first_child(parent: ET.Element, *local_names: str) -> Optional[ET.Element]:
    """Return the first direct child whose local name matches."""
    for child in list(parent):
        if _xml_local_name(child.tag) in local_names:
            return child
    return None


def _xml_find_descendant(parent: ET.Element, *local_names: str) -> Optional[ET.Element]:
    """Return the first descendant whose local name matches."""
    wanted = set(local_names)
    for child in parent.iter():
        if child is parent:
            continue
        if _xml_local_name(child.tag) in wanted:
            return child
    return None


def _xml_element_text(element: Optional[ET.Element]) -> str:
    """Extract readable text from an XML element."""
    if element is None:
        return ""
    text = "".join(element.itertext()).strip()
    return strip_tags(extract_cdata(text))


def _extract_atom_link(entry: ET.Element, feed_url: str) -> str:
    """Extract the most useful Atom entry link."""
    links = _xml_child_elements(entry, "link")
    for link_el in links:
        rel = link_el.attrib.get("rel", "alternate")
        href = link_el.attrib.get("href", "").strip()
        if href and rel in ("alternate", ""):
            return resolve_link(href, feed_url)
    for link_el in links:
        href = link_el.attrib.get("href", "").strip()
        if href:
            return resolve_link(href, feed_url)
    fallback = _xml_first_child(entry, "link")
    return resolve_link(_xml_element_text(fallback), feed_url)


def is_probably_feed(content: str, content_type: str = "") -> bool:
    """Heuristic check for RSS/Atom/RDF feed responses."""
    text = content.lstrip().lower()
    ctype = content_type.lower()
    if "xml" in ctype or "atom" in ctype or "rss" in ctype:
        return True
    return any(marker in text for marker in ("<rss", "<feed", "<rdf:rdf", "<rdf"))


def validate_article_domain(article_link: str, source: Dict[str, Any]) -> bool:
    """Validate that article links from mirror sources point to expected domains.
    
    Sources with 'expected_domains' field will have their article links checked.
    Returns True if valid or if no domain restriction is set.
    """
    expected = source.get("expected_domains")
    if not expected:
        return True
    if not article_link:
        return False
    from urllib.parse import urlparse
    domain = urlparse(article_link).hostname or ""
    return any(domain == d or domain.endswith("." + d) for d in expected)


def resolve_link(link: str, base_url: str) -> str:
    """Resolve relative links against the feed URL. Rejects non-HTTP(S) schemes."""
    if not link:
        return link
    if link.startswith(("http://", "https://")):
        return link
    resolved = urljoin(base_url, link)
    if not resolved.startswith(("http://", "https://")):
        return ""  # reject javascript:, data:, etc.
    return resolved


def parse_feed_feedparser(content: str, cutoff: datetime, feed_url: str) -> List[Dict[str, Any]]:
    """Parse feed using feedparser library."""
    articles = []
    
    try:
        feed = feedparser.parse(content)
        
        for entry in feed.entries[:MAX_ARTICLES_PER_FEED]:
            title = entry.get('title', '').strip()
            link = entry.get('link', '').strip()
            
            # Try multiple date fields
            pub_date = None
            for date_field in ['published_parsed', 'updated_parsed']:
                if hasattr(entry, date_field) and getattr(entry, date_field):
                    try:
                        pub_date = datetime(*getattr(entry, date_field)[:6], tzinfo=timezone.utc)
                        break
                    except (TypeError, ValueError):
                        continue
                        
            # Fallback to string parsing
            if pub_date is None:
                for date_field in ['published', 'updated']:
                    if hasattr(entry, date_field) and getattr(entry, date_field):
                        pub_date = parse_date_regex(getattr(entry, date_field))
                        if pub_date:
                            break
                            
            if title and link and pub_date and pub_date >= cutoff:
                articles.append({
                    "title": title[:200],
                    "link": resolve_link(link, feed_url),
                    "date": pub_date.isoformat(),
                })
                
    except Exception as e:
        logging.debug(f"feedparser parsing failed: {e}")
        
    return articles


def parse_feed_xml(content: str, cutoff: datetime, feed_url: str) -> List[Dict[str, Any]]:
    """Parse feed using XML parsing as a general fallback."""
    articles = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logging.debug(f"XML fallback parsing failed: {e}")
        return articles

    root_name = _xml_local_name(root.tag)
    item_nodes: List[ET.Element] = []
    atom_mode = False

    if root_name == "rss":
        channel = _xml_first_child(root, "channel")
        item_nodes = _xml_child_elements(channel, "item") if channel is not None else []
    elif root_name == "feed":
        atom_mode = True
        item_nodes = _xml_child_elements(root, "entry")
    elif root_name == "RDF":
        item_nodes = [child for child in list(root) if _xml_local_name(child.tag) == "item"]
    else:
        entry_nodes = [child for child in list(root) if _xml_local_name(child.tag) == "entry"]
        item_nodes = [child for child in list(root) if _xml_local_name(child.tag) == "item"]
        if entry_nodes:
            atom_mode = True
            item_nodes = entry_nodes

    for item in item_nodes[:MAX_ARTICLES_PER_FEED]:
        if atom_mode:
            title = _xml_element_text(_xml_first_child(item, "title"))
            link = _extract_atom_link(item, feed_url)
            date_el = _xml_first_child(item, "updated", "published")
            pub = parse_date_regex(_xml_element_text(date_el))
        else:
            title = _xml_element_text(_xml_first_child(item, "title"))
            link = resolve_link(
                _xml_element_text(_xml_first_child(item, "link")),
                feed_url,
            )
            date_el = _xml_first_child(item, "pubDate", "date", "published", "updated")
            if date_el is None:
                date_el = _xml_find_descendant(item, "date")
            pub = parse_date_regex(_xml_element_text(date_el))

        if title and link and pub and pub >= cutoff:
            articles.append({
                "title": title[:200],
                "link": link,
                "date": pub.isoformat(),
            })

    return articles[:MAX_ARTICLES_PER_FEED]


def parse_feed(content: str, cutoff: datetime, feed_url: str) -> List[Dict[str, Any]]:
    """Parse feed using best available method."""
    if HAS_FEEDPARSER:
        articles = parse_feed_feedparser(content, cutoff, feed_url)
        if articles:
            return articles
        logging.debug("feedparser returned no articles, trying XML fallback")

    if not is_probably_feed(content):
        return []

    return parse_feed_xml(content, cutoff, feed_url)


def _load_rss_cache() -> Dict[str, Any]:
    """Load RSS ETag/Last-Modified cache."""
    try:
        with open(RSS_CACHE_PATH, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_rss_cache(cache: Dict[str, Any]) -> None:
    """Save RSS ETag/Last-Modified cache."""
    try:
        with open(RSS_CACHE_PATH, 'w') as f:
            json.dump(cache, f)
    except Exception as e:
        logging.warning(f"Failed to save RSS cache: {e}")


# Module-level cache, loaded once per run
# Protected by _rss_cache_lock for thread-safe access
_rss_cache: Optional[Dict[str, Any]] = None
_rss_cache_dirty = False
_rss_cache_lock = threading.RLock()  # Reentrant lock to allow nested acquisition


def _get_rss_cache(no_cache: bool = False) -> Dict[str, Any]:
    global _rss_cache
    with _rss_cache_lock:
        if _rss_cache is None:
            _rss_cache = {} if no_cache else _load_rss_cache()
        return _rss_cache


def _flush_rss_cache() -> None:
    global _rss_cache, _rss_cache_dirty
    with _rss_cache_lock:
        if _rss_cache_dirty and _rss_cache is not None:
            _save_rss_cache(_rss_cache)
            _rss_cache_dirty = False


def fetch_feed_with_retry(source: Dict[str, Any], cutoff: datetime, no_cache: bool = False) -> Dict[str, Any]:
    """Fetch RSS feed with retry mechanism and conditional requests."""
    source_id = source["id"]
    name = source["name"]
    url = source["url"]
    priority = normalize_priority(source.get("priority"))
    topic = str(source.get("topic") or "")
    request_log: List[Dict[str, Any]] = []
    started_at = time.monotonic()
    
    global _rss_cache, _rss_cache_dirty
    
    for attempt in range(RETRY_COUNT + 1):
        request_started_at = time.monotonic()
        try:
            req_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"}
            
            # Add conditional headers from cache (thread-safe)
            with _rss_cache_lock:
                cache = _rss_cache if _rss_cache is not None else {}
                cache_entry = cache.get(url)
            now = time.time()
            ttl_seconds = RSS_CACHE_TTL_HOURS * 3600
            
            if cache_entry and not no_cache and (now - cache_entry.get("ts", 0)) < ttl_seconds:
                if cache_entry.get("etag"):
                    req_headers["If-None-Match"] = cache_entry["etag"]
                if cache_entry.get("last_modified"):
                    req_headers["If-Modified-Since"] = cache_entry["last_modified"]
            
            req = Request(url, headers=req_headers)
            try:
                with fetch_with_redirects(url, req_headers, TIMEOUT) as resp:
                    # Update cache with response headers (thread-safe)
                    etag = resp.headers.get("ETag")
                    last_mod = resp.headers.get("Last-Modified")
                    if etag or last_mod:
                        with _rss_cache_lock:
                            if _rss_cache is None:
                                _rss_cache = {}
                            _rss_cache[url] = {"etag": etag, "last_modified": last_mod, "ts": now}
                            _rss_cache_dirty = True
                    
                    final_url = resp.url if hasattr(resp, 'url') else url
                    content = resp.read().decode("utf-8", errors="replace")
            except URLError as e:
                if hasattr(e, 'code') and e.code == 304:
                    logging.info(f"⏭ {name}: not modified (304)")
                    request_log.append(
                        build_request_trace(
                            url,
                            time.monotonic() - request_started_at,
                            status="ok",
                            method="GET",
                            attempt=attempt + 1,
                            result="not_modified",
                        )
                    )
                    return {
                        "source_id": source_id,
                        "source_type": "rss",
                        "name": name,
                        "url": url,
                        "priority": priority,
                        "topic": topic,
                        "status": "ok",
                        "attempts": attempt + 1,
                        "elapsed_s": round(time.monotonic() - started_at, 3),
                        "not_modified": True,
                        "items": 0,
                        "count": 0,
                        "articles": [],
                        "request_traces": request_log,
                        "failed_items": [],
                    }
                raise
            request_log.append(
                build_request_trace(
                    url,
                    time.monotonic() - request_started_at,
                    status="ok",
                    method="GET",
                    attempt=attempt + 1,
                )
            )
                
            articles = parse_feed(content, cutoff, final_url)
            
            # Tag articles with the source topic and validate domains
            validated_articles = []
            for article in articles:
                article["topic"] = topic
                if validate_article_domain(article.get("link", ""), source):
                    validated_articles.append(article)
                else:
                    logging.warning(f"⚠️ {name}: rejected article with unexpected domain: {article.get('link', '')}")
            articles = validated_articles
            
            return {
                "source_id": source_id,
                "source_type": "rss",
                "name": name,
                "url": url,
                "priority": priority,
                "topic": topic,
                "status": "ok",
                "attempts": attempt + 1,
                "elapsed_s": round(time.monotonic() - started_at, 3),
                "items": len(articles),
                "count": len(articles),
                "articles": articles,
                "request_traces": request_log,
                "failed_items": [],
            }
            
        except Exception as e:
            error_msg = str(e)[:100]
            request_log.append(
                build_request_trace(
                    url,
                    time.monotonic() - request_started_at,
                    status="error",
                    method="GET",
                    attempt=attempt + 1,
                    error=error_msg,
                )
            )
            logging.debug(f"Attempt {attempt + 1} failed for {name}: {error_msg}")
            
            if attempt < RETRY_COUNT and is_retryable_rss_error(e):
                time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                continue
            else:
                return {
                    "source_id": source_id,
                    "source_type": "rss",
                    "name": name,
                    "url": url,
                    "priority": priority,
                    "topic": topic,
                    "status": "error",
                    "attempts": attempt + 1,
                    "error": error_msg,
                    "elapsed_s": round(time.monotonic() - started_at, 3),
                    "items": 0,
                    "count": 0,
                    "articles": [],
                    "request_traces": request_log,
                    "failed_items": [normalize_failed_item(source_id, error_msg, time.monotonic() - started_at)],
                }


def load_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Load RSS sources from dedicated RSS configuration with overlay support."""
    try:
        from config_loader import load_merged_rss_sources
    except ImportError:
        import sys
        sys.path.append(str(Path(__file__).parent))
        from config_loader import load_merged_rss_sources

    all_sources = load_merged_rss_sources(defaults_dir, config_dir)
    rss_sources = [source for source in all_sources if source.get("enabled", True)]
    logging.info(f"Loaded {len(rss_sources)} enabled RSS sources")
    return rss_sources

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parallel RSS/Atom feed fetcher for news-hotspots. "
                   "Fetches enabled RSS sources from unified configuration, "
                   "filters by time window, and outputs structured article data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-rss.py
    python3 fetch-rss.py --defaults config/defaults --config workspace/config --hours 48 -o results.json
        """
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory with skill defaults (default: config/defaults)")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays (optional)")
    parser.add_argument("--hours", type=int, default=48, help="Time window in hours for articles (default: 48)")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path (default: auto-generated temp file)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--no-cache", action="store_true", help="Bypass ETag/Last-Modified conditional request cache")
    parser.add_argument("--force", action="store_true", help="Force re-fetch even if cached output exists")

    return parser.parse_args()


def main():
    """Main RSS fetching function."""
    args = parse_args()
    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)

    # Resume support: skip if output exists, is valid JSON, and < 1 hour old
    if args.output and args.output.exists() and not args.force:
        try:
            age_seconds = time.time() - args.output.stat().st_mtime
            if age_seconds < 3600:
                with open(args.output, 'r') as f:
                    json.load(f)  # validate JSON
                logger.info(f"Skipping (cached output exists): {args.output}")
                return 0
        except (json.JSONDecodeError, OSError):
            pass
    
    # Auto-generate unique output path if not specified
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-rss-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)
    
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
        step_started_at = time.monotonic()
        
        sources = load_sources(args.defaults, effective_config_dir)
        
        if not sources:
            logger.warning("No RSS sources found or all disabled")
            
        logger.info(f"Fetching {len(sources)} RSS feeds (window: {args.hours}h)")
        
        # Check feedparser availability
        if HAS_FEEDPARSER:
            logger.debug("Using feedparser library for parsing")
        else:
            logger.info("feedparser not available, using XML fallback parsing")
        
        # Initialize cache
        _get_rss_cache(no_cache=args.no_cache)
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(fetch_feed_with_retry, source, cutoff, args.no_cache): source 
                      for source in sources}
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                if result["status"] == "ok":
                    logger.debug(f"✅ {result['name']}: {result['count']} articles")
                else:
                    logger.debug(f"❌ {result['name']}: {result['error']}")

        # Flush conditional request cache
        _flush_rss_cache()
        
        # Sort: higher priority first, then by article count
        results.sort(key=lambda x: (-normalize_priority(x.get("priority")), -x.get("count", 0)))

        ok_count = sum(1 for r in results if r["status"] == "ok")
        total_articles = sum(r.get("count", 0) for r in results)
        articles = [article for result in results for article in result.get("articles", []) if isinstance(article, dict)]
        failed_items = [
            normalize_failed_item(result.get("source_id"), result.get("error"), result.get("elapsed_s"))
            for result in results
            if result.get("status") != "ok" and result.get("error")
        ]

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "rss",
            "articles": articles,
        }
        request_traces = [trace for result in results for trace in result.get("request_traces", []) if isinstance(trace, dict)]
        meta = build_step_meta(
            step_key="rss",
            status="ok" if ok_count == len(results) and total_articles > 0 else ("partial" if ok_count > 0 and total_articles > 0 else "error"),
            elapsed_s=time.monotonic() - step_started_at,
            items=total_articles,
            calls_total=len(results),
            calls_ok=ok_count,
            failed_items=failed_items,
            request_traces=request_traces,
        )
        write_result_with_meta(args.output, output, meta)

        logger.info(f"✅ Done: {ok_count}/{len(results)} feeds ok, "
                   f"{total_articles} articles → {args.output}")
        
        return 0 if total_articles > 0 else 1
        
    except Exception as e:
        logger.error(f"💥 RSS fetch failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
