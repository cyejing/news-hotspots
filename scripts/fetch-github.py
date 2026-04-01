#!/usr/bin/env python3
"""
GitHub source_type 抓取脚本。

职责：
- 读取 `github.json` 中启用的 GitHub 仓库配置
- 顺序抓取 release / repo 动态
- 标准化为统一 `articles`
- 把失败请求、重试结果和耗时写入 `*.meta.json`

执行逻辑：
1. 加载 runtime 与 GitHub source 配置
2. 按 cooldown 顺序请求各仓库
3. 将命中的 release 标准化为 article
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存标准化后的 GitHub article 列表，供 `merge-sources.py` 合并
- `<step>.meta.json`
  只保存抓取诊断，供 `run-pipeline.py` 聚合和 `source-health.py` 诊断

环境变量：
- `GITHUB_TOKEN`
  可选，用于提高 GitHub API 速率限制
"""

import json
import re
import sys
import os
import argparse
import logging
import time
import tempfile
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from pathlib import Path
from typing import Dict, List, Any, Optional

try:
    from config_loader import load_merged_runtime_config, load_merged_github_sources
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config, load_merged_github_sources

try:
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta

TIMEOUT = 25
MAX_RELEASES_PER_REPO = 20
RETRY_COUNT = 2
RETRY_DELAY = 2.0  # seconds
GITHUB_CACHE_PATH = "/tmp/news-hotspots-github-cache.json"
GITHUB_CACHE_TTL_HOURS = 24
GITHUB_COOLDOWN_DEFAULT = 2.0


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global TIMEOUT, MAX_RELEASES_PER_REPO, RETRY_COUNT, RETRY_DELAY, GITHUB_CACHE_PATH, GITHUB_CACHE_TTL_HOURS, GITHUB_COOLDOWN_DEFAULT
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("github", {})
    diagnostics_config = runtime.get("diagnostics", {})
    cache_config = runtime.get("cache", {})
    TIMEOUT = int(fetch_config.get("request_timeout_s", TIMEOUT) or TIMEOUT)
    MAX_RELEASES_PER_REPO = int(fetch_config.get("releases_per_repo", MAX_RELEASES_PER_REPO) or MAX_RELEASES_PER_REPO)
    RETRY_COUNT = int(fetch_config.get("retry_count", RETRY_COUNT) or RETRY_COUNT)
    RETRY_DELAY = float(fetch_config.get("retry_delay_s", RETRY_DELAY) or RETRY_DELAY)
    GITHUB_CACHE_TTL_HOURS = int(fetch_config.get("cache_ttl_hours", GITHUB_CACHE_TTL_HOURS) or GITHUB_CACHE_TTL_HOURS)
    GITHUB_COOLDOWN_DEFAULT = float(fetch_config.get("cooldown_s", GITHUB_COOLDOWN_DEFAULT) or 0)
    GITHUB_CACHE_PATH = str(cache_config.get("github_cache_path", GITHUB_CACHE_PATH) or GITHUB_CACHE_PATH)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def normalize_priority(priority: Any, default: int = 3) -> int:
    """Normalize source priority into a 1-10 score."""
    if isinstance(priority, bool):
        return 8 if priority else default
    try:
        value = int(priority)
    except (TypeError, ValueError):
        return default
    return max(1, min(10, value))


def get_github_cooldown_seconds() -> float:
    """Get sequential request cooldown for GitHub release fetches."""
    return max(0.0, float(GITHUB_COOLDOWN_DEFAULT))


def is_retryable_github_error(exc: Exception) -> bool:
    """Retry only transient GitHub failures."""
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


def strip_markdown(text: str) -> str:
    """Strip basic markdown formatting from text."""
    if not text:
        return ""
    
    # Remove links [text](url) -> text
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    # Remove bold/italic **text** or *text* -> text
    text = re.sub(r'\*+([^*]+)\*+', r'\1', text)
    # Remove headers ### -> 
    text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    # Remove code blocks ```
    text = re.sub(r'```[^`]*```', '', text, flags=re.DOTALL)
    # Remove inline code `text` -> text
    text = re.sub(r'`([^`]+)`', r'\1', text)
    
    return text.strip()


def truncate_summary(text: str, max_chars: int = 500) -> str:
    """Truncate text to specified length with ellipsis."""
    if not text:
        return ""
    
    # Strip markdown first
    clean_text = strip_markdown(text)
    
    # Remove extra whitespace
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    
    if len(clean_text) <= max_chars:
        return clean_text
    
    # Find last space before limit
    truncated = clean_text[:max_chars]
    last_space = truncated.rfind(' ')
    
    if last_space > max_chars * 0.8:  # Don't cut too much
        truncated = truncated[:last_space]
    
    return truncated + "..."


def resolve_github_token() -> Optional[str]:
    """Resolve GitHub token from $GITHUB_TOKEN only."""
    token = os.environ.get("GITHUB_TOKEN")
    logging.info(f"🔍 GITHUB_TOKEN: {'set' if token else 'not set'}")
    if token:
        logging.info("🔑 Using GitHub token (5000 req/hr)")
        return token

    logging.warning("⚠️ No GitHub token found — rate limit 60 req/hr (22 repos may fail)")
    logging.warning("  Set $GITHUB_TOKEN to improve rate limits")
    return None


def parse_github_date(date_str: str) -> Optional[datetime]:
    """Parse GitHub ISO date string."""
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


def get_repo_name(repo: str) -> str:
    """Extract repository name from owner/repo format."""
    return repo.split('/')[-1] if '/' in repo else repo


def _load_github_cache() -> Dict[str, Any]:
    """Load GitHub ETag/Last-Modified cache."""
    try:
        with open(GITHUB_CACHE_PATH, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_github_cache(cache: Dict[str, Any]) -> None:
    """Save GitHub ETag/Last-Modified cache."""
    try:
        with open(GITHUB_CACHE_PATH, 'w') as f:
            json.dump(cache, f)
    except Exception as e:
        logging.warning(f"Failed to save GitHub cache: {e}")


_github_cache: Optional[Dict[str, Any]] = None
_github_cache_dirty = False


def _get_github_cache(no_cache: bool = False) -> Dict[str, Any]:
    global _github_cache
    if _github_cache is None:
        _github_cache = {} if no_cache else _load_github_cache()
    return _github_cache


def _flush_github_cache() -> None:
    global _github_cache_dirty
    if _github_cache_dirty and _github_cache is not None:
        _save_github_cache(_github_cache)
        _github_cache_dirty = False


def fetch_releases_with_retry(source: Dict[str, Any], cutoff: datetime, github_token: Optional[str] = None, no_cache: bool = False) -> Dict[str, Any]:
    """Fetch GitHub releases with retry mechanism and conditional requests."""
    source_id = source["id"]
    name = source["name"]
    repo = source["repo"]
    priority = normalize_priority(source.get("priority"))
    topic = str(source.get("topic") or "")
    started_at = time.monotonic()
    request_log: List[Dict[str, Any]] = []
    
    repo_name = get_repo_name(repo)
    api_url = f"https://api.github.com/repos/{repo}/releases"
    
    # Setup headers
    headers = {
        "User-Agent": "NewsHotspots/2.0",
        "Accept": "application/vnd.github.v3+json",
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    
    # Add conditional headers from cache
    global _github_cache_dirty
    cache = _get_github_cache(no_cache)
    cache_entry = cache.get(api_url)
    now = time.time()
    ttl_seconds = GITHUB_CACHE_TTL_HOURS * 3600
    
    if cache_entry and not no_cache and (now - cache_entry.get("ts", 0)) < ttl_seconds:
        if cache_entry.get("etag"):
            headers["If-None-Match"] = cache_entry["etag"]
        if cache_entry.get("last_modified"):
            headers["If-Modified-Since"] = cache_entry["last_modified"]
    
    for attempt in range(RETRY_COUNT + 1):
        request_started_at = time.monotonic()
        try:
            req = Request(api_url, headers=headers)
            try:
                with urlopen(req, timeout=TIMEOUT) as resp:
                    # Update cache
                    etag = resp.headers.get("ETag")
                    last_mod = resp.headers.get("Last-Modified")
                    if etag or last_mod:
                        cache[api_url] = {"etag": etag, "last_modified": last_mod, "ts": now}
                        _github_cache_dirty = True
                    
                    content = resp.read().decode("utf-8", errors="replace")
                    releases_data = json.loads(content)
            except HTTPError as e:
                if e.code == 304:
                    logging.info(f"⏭ {name}: not modified (304)")
                    request_log.append(
                        build_request_trace(
                            api_url,
                            time.monotonic() - request_started_at,
                            status="ok",
                            method="GET",
                            attempt=attempt + 1,
                            result="not_modified",
                        )
                    )
                    return {
                        "source_id": source_id,
                        "source_type": "github",
                        "name": name,
                        "repo": repo,
                        "priority": priority,
                        "topic": topic,
                        "status": "ok",
                        "attempts": attempt + 1,
                        "elapsed_s": round(time.monotonic() - started_at, 3),
                        "not_modified": True,
                        "count": 0,
                        "articles": [],
                        "request_traces": request_log,
                        "failed_items": [],
                    }
                raise

            request_log.append(
                build_request_trace(
                    api_url,
                    time.monotonic() - request_started_at,
                    status="ok",
                    method="GET",
                    attempt=attempt + 1,
                )
            )
            
            articles = []
            for release in releases_data[:MAX_RELEASES_PER_REPO]:
                # Skip drafts and prereleases optionally
                if release.get("draft", False):
                    continue
                
                published_at = release.get("published_at")
                if not published_at:
                    continue
                
                pub_date = parse_github_date(published_at)
                if not pub_date or pub_date < cutoff:
                    continue
                
                tag_name = release.get("tag_name", "")
                title = f"{repo_name} {tag_name}"
                link = release.get("html_url", "")
                body = release.get("body", "")
                summary = truncate_summary(body, 500)
                
                if title and link:
                    articles.append({
                        "title": title,
                        "link": link,
                        "date": pub_date.isoformat(),
                        "summary": summary,
                        "topic": topic,
                    })
            
            return {
                "source_id": source_id,
                "source_type": "github",
                "name": name,
                "repo": repo,
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
                    api_url,
                    time.monotonic() - request_started_at,
                    status="error",
                    method="GET",
                    attempt=attempt + 1,
                    error=error_msg,
                )
            )
            logging.debug(f"Attempt {attempt + 1} failed for {name}: {error_msg}")
            
            if attempt < RETRY_COUNT and is_retryable_github_error(e):
                # Exponential backoff with jitter for API rate limits
                delay = RETRY_DELAY * (2 ** attempt)
                time.sleep(delay)
                continue
            else:
                return {
                    "source_id": source_id,
                    "source_type": "github",
                    "name": name,
                    "repo": repo,
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
    """Load GitHub sources from dedicated GitHub configuration with overlay support."""
    all_sources = load_merged_github_sources(defaults_dir, config_dir)
    github_sources = [source for source in all_sources if source.get("enabled", True) and source.get("repo")]
    logging.info(f"Loaded {len(github_sources)} enabled GitHub sources")
    return github_sources

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sequential GitHub releases fetcher for news-hotspots. "
                   "Fetches enabled GitHub sources from unified configuration, "
                    "filters by time window, and outputs structured release data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-github.py
    python3 fetch-github.py --defaults config/defaults --config workspace/config --hours 168 -o results.json
    
Environment Variables:
    GITHUB_TOKEN    GitHub token (optional, improves rate limits)
        """
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory with skill defaults (default: config/defaults)")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays (optional)")
    parser.add_argument("--hours", type=int, default=168, help="Time window in hours for releases (default: 168 = 1 week)")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path (default: auto-generated temp file)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--no-cache", action="store_true", help="Bypass ETag/Last-Modified conditional request cache")
    parser.add_argument("--force", action="store_true", help="Force re-fetch even if cached output exists")

    return parser.parse_args()


def main():
    """Main GitHub releases fetching function."""
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
                    json.load(f)
                logger.info(f"Skipping (cached output exists): {args.output}")
                return 0
        except (json.JSONDecodeError, OSError):
            pass
    
    # Auto-generate unique output path if not specified
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-github-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)
    
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
        
        sources = load_sources(args.defaults, effective_config_dir)
        
        if not sources:
            logger.warning("No GitHub sources found or all disabled")
        
        logger.info(f"Fetching {len(sources)} GitHub repositories (window: {args.hours}h)")
        
        # Resolve GitHub token ($GITHUB_TOKEN → unauthenticated)
        github_token = resolve_github_token()
        
        # Initialize cache
        _get_github_cache(no_cache=args.no_cache)
        
        cooldown_s = get_github_cooldown_seconds()
        logger.info("GitHub sequential cooldown: %.1fs", cooldown_s)

        results = []
        last_finished_at: Optional[float] = None
        for source in sources:
            if last_finished_at is not None and cooldown_s > 0:
                elapsed_since_last = time.time() - last_finished_at
                if elapsed_since_last < cooldown_s:
                    time.sleep(cooldown_s - elapsed_since_last)

            result = fetch_releases_with_retry(source, cutoff, github_token, args.no_cache)
            results.append(result)
            last_finished_at = time.time()

            if result["status"] == "ok":
                logger.debug(f"✅ {result['name']}: {result['count']} releases")
            else:
                logger.debug(f"❌ {result['name']}: {result['error']}")

        # Flush conditional request cache
        _flush_github_cache()
        
        # Sort: higher priority first, then by release count
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
            "source_type": "github",
            "articles": articles,
        }
        request_traces = [trace for result in results for trace in result.get("request_traces", []) if isinstance(trace, dict)]
        meta = build_step_meta(
            step_key="github",
            status="ok" if ok_count == len(results) and total_articles > 0 else ("partial" if ok_count > 0 and total_articles > 0 else "error"),
            elapsed_s=sum(float(result.get("elapsed_s", 0) or 0) for result in results),
            items=total_articles,
            calls_total=len(results),
            calls_ok=ok_count,
            failed_items=failed_items,
            request_traces=request_traces,
        )
        write_result_with_meta(args.output, output, meta)

        logger.info(f"✅ Done: {ok_count}/{len(results)} repos ok, "
                   f"{total_articles} releases → {args.output}")
        
        return 0 if total_articles > 0 else 1
        
    except Exception as e:
        logger.error(f"💥 GitHub fetch failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
