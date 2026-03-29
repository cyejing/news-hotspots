#!/usr/bin/env python3
"""
Fetch GitHub releases from unified sources configuration.

Reads sources.json, filters GitHub sources, fetches releases sequentially with retry
mechanism, and outputs structured JSON with releases tagged by topics.

Usage:
    python3 fetch-github.py [--config CONFIG_DIR] [--hours 48] [--output FILE] [--verbose]
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
from urllib.error import HTTPError
from urllib.parse import quote
from pathlib import Path
from typing import Dict, List, Any, Optional

TIMEOUT = 60
MAX_RELEASES_PER_REPO = 20
RETRY_COUNT = 2
RETRY_DELAY = 2.0  # seconds
GITHUB_CACHE_PATH = "/tmp/news-hotspots-github-cache.json"
GITHUB_CACHE_TTL_HOURS = 24
GITHUB_COOLDOWN_ENV = "NEWS_HOTSPOTS_GITHUB_COOLDOWN_SECONDS"
GITHUB_COOLDOWN_DEFAULT = 2.0


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
    raw = os.environ.get(GITHUB_COOLDOWN_ENV, str(GITHUB_COOLDOWN_DEFAULT))
    try:
        return max(0.0, float(raw))
    except ValueError:
        return GITHUB_COOLDOWN_DEFAULT


def _b64url(data: bytes) -> str:
    """Base64url encode without padding."""
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def _generate_github_app_token(app_id: str, install_id: str, key_file: str) -> str:
    """Generate a GitHub App installation token using JWT (RS256 via openssl).
    
    No external scripts or pip dependencies required — uses openssl CLI for RSA signing.
    Returns the token string, or empty string on failure.
    """
    import subprocess as _sp

    with open(key_file) as f:
        private_key = f.read()

    # Build JWT
    now = int(time.time())
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({"iat": now - 60, "exp": now + 600, "iss": app_id}).encode())
    signing_input = f"{header}.{payload}"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
        f.write(private_key)
        tmp_key = f.name
    try:
        result = _sp.run(
            ['openssl', 'dgst', '-sha256', '-sign', tmp_key],
            input=signing_input.encode(), capture_output=True, timeout=10,
        )
        if result.returncode != 0:
            logging.debug(f"openssl sign failed: {result.stderr.decode()}")
            return ""
        signature = _b64url(result.stdout)
    finally:
        os.unlink(tmp_key)

    jwt = f"{signing_input}.{signature}"

    # Exchange JWT for installation token
    req = Request(
        f"https://api.github.com/app/installations/{install_id}/access_tokens",
        method='POST',
        headers={
            'Authorization': f'Bearer {jwt}',
            'Accept': 'application/vnd.github+json',
            'User-Agent': 'news-hotspots',
        },
    )
    with urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    return data.get("token", "")


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
    """Resolve GitHub token from multiple sources, in priority order:
    
    1. $GITHUB_TOKEN env var (PAT or pre-generated App token)
    2. GitHub App installation token (auto-generated from App credentials)
    3. `gh auth token` CLI fallback
    4. None (unauthenticated, 60 req/hr)
    """
    # 1. Environment variable (PAT or externally-set App token)
    token = os.environ.get("GITHUB_TOKEN")
    logging.info(f"🔍 GITHUB_TOKEN: {'set' if token else 'not set'}")
    if token:
        if token.startswith("ghp_"):
            logging.info("🔑 Using GitHub PAT (5000 req/hr)")
        elif token.startswith("ghs_"):
            logging.info("🔑 Using GitHub App installation token (5000 req/hr)")
        else:
            logging.info("🔑 Using GitHub token (5000 req/hr)")
        return token
    
    # 2. GitHub App auto-generation (requires GH_APP_ID, GH_APP_INSTALL_ID, GH_APP_KEY_FILE env vars)
    #    Generates a short-lived installation token using JWT + GitHub API. No external scripts needed.
    app_id = os.environ.get("GH_APP_ID")
    install_id = os.environ.get("GH_APP_INSTALL_ID")
    key_file = os.environ.get("GH_APP_KEY_FILE")
    logging.info(f"🔍 GH_APP_ID: {'set' if app_id else 'not set'}")
    logging.info(f"🔍 GH_APP_INSTALL_ID: {'set' if install_id else 'not set'}")
    logging.info(f"🔍 GH_APP_KEY_FILE: {'set' if key_file else 'not set'}{' (file exists)' if key_file and os.path.exists(key_file) else ' (file missing)' if key_file else ''}")
    
    if app_id and install_id and key_file and os.path.exists(key_file):
        try:
            token = _generate_github_app_token(app_id, install_id, key_file)
            if token:
                logging.info("🔑 GitHub App token auto-generated (5000 req/hr)")
                return token
        except Exception as e:
            logging.debug(f"GitHub App token generation failed: {e}")
    
    # 3. gh CLI fallback
    logging.info("🔍 Trying gh CLI fallback...")
    try:
        import subprocess
        result = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, timeout=5
        )
        token = result.stdout.strip()
        if token and result.returncode == 0:
            logging.info("🔑 Using gh CLI token (5000 req/hr)")
            return token
        else:
            logging.info(f"🔍 gh auth token: exit={result.returncode}, output={'set' if token else 'empty'}")
    except Exception as e:
        logging.info(f"🔍 gh CLI not available: {e}")
    
    # 4. Unauthenticated
    logging.warning("⚠️ No GitHub token found — rate limit 60 req/hr (22 repos may fail)")
    logging.warning("  Set $GITHUB_TOKEN or install GitHub App credentials to fix this")
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
    topics = source["topics"]
    
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
                    return {
                        "source_id": source_id,
                        "source_type": "github",
                        "name": name,
                        "repo": repo,
                        "priority": priority,
                        "topics": topics,
                        "status": "ok",
                        "attempts": attempt + 1,
                        "not_modified": True,
                        "count": 0,
                        "articles": [],
                    }
                raise
            
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
                        "topics": topics[:],
                    })
            
            return {
                "source_id": source_id,
                "source_type": "github",
                "name": name,
                "repo": repo,
                "priority": priority,
                "topics": topics,
                "status": "ok",
                "attempts": attempt + 1,
                "items": len(articles),
                "count": len(articles),
                "articles": articles,
            }
            
        except Exception as e:
            error_msg = str(e)[:100]
            logging.debug(f"Attempt {attempt + 1} failed for {name}: {error_msg}")
            
            if attempt < RETRY_COUNT:
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
                    "topics": topics,
                    "status": "error",
                    "attempts": attempt + 1,
                    "error": error_msg,
                    "items": 0,
                    "count": 0,
                    "articles": [],
                }


def load_sources(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Load GitHub sources from unified configuration with overlay support."""
    try:
        from config_loader import load_merged_sources
    except ImportError:
        # Fallback for relative import
        import sys
        sys.path.append(str(Path(__file__).parent))
        from config_loader import load_merged_sources
    
    # Load merged sources from defaults + optional user overlay
    all_sources = load_merged_sources(defaults_dir, config_dir)
    
    # Filter GitHub sources that are enabled
    github_sources = []
    for source in all_sources:
        if source.get("type") == "github" and source.get("enabled", True):
            # Validate required fields
            if not source.get("repo"):
                logging.warning(f"GitHub source {source.get('id', 'unknown')} missing repo field, skipping")
                continue
            github_sources.append(source)
    
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
    GITHUB_TOKEN    GitHub personal access token (optional, improves rate limits)
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
        
        sources = load_sources(args.defaults, args.config)
        
        if not sources:
            logger.warning("No GitHub sources found or all disabled")
        
        logger.info(f"Fetching {len(sources)} GitHub repositories (window: {args.hours}h)")
        
        # Resolve GitHub token (PAT → App → gh CLI → unauthenticated)
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

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source_type": "github",
            "defaults_dir": str(args.defaults),
            "config_dir": str(args.config) if args.config else None,
            "hours": args.hours,
            "github_token_used": github_token is not None,
            "cooldown_s": cooldown_s,
            "calls_total": len(results),
            "calls_ok": ok_count,
            "calls_kind": "sources",
            "items_total": total_articles,
            "sources_total": len(results),
            "sources_ok": ok_count,
            "total_articles": total_articles,
            "sources": results,
        }

        # Write output
        json_str = json.dumps(output, ensure_ascii=False, indent=2)
        with open(args.output, "w", encoding='utf-8') as f:
            f.write(json_str)

        logger.info(f"✅ Done: {ok_count}/{len(results)} repos ok, "
                   f"{total_articles} releases → {args.output}")
        
        return 0
        
    except Exception as e:
        logger.error(f"💥 GitHub fetch failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
