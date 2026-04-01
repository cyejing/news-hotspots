#!/usr/bin/env python3
"""
GitHub Trending 抓取脚本。

职责：
- 读取 `topics.json` 中的 GitHub Trending 查询
- 调用 GitHub Search API 抓取热门仓库
- 将仓库结果直接标准化为统一 `articles`
- 记录失败请求、单请求耗时与 step 级诊断信息

执行逻辑：
1. 加载 runtime 与 topics 配置
2. 逐个 topic 执行 GitHub 搜索查询
3. 将命中的仓库转换为统一 article 结构
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存标准化后的热门仓库 article 列表，供 `merge-sources.py` 消费
- `<step>.meta.json`
  只保存抓取诊断，包括总耗时、成功/失败请求数、失败明细和慢请求统计

环境变量：
- `GITHUB_TOKEN`
  可选，用于提高 GitHub API 速率限制
"""

import json
import sys
import os
import argparse
import logging
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

try:
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config
    from step_contract import build_request_trace, build_step_meta, configure_slow_request_thresholds, normalize_failed_item, write_result_with_meta

# ==================== 常量配置 ====================
TIMEOUT = 60  # 请求超时时间（秒）
USER_AGENT = "NewsHotspots/3.0 (bot; +https://github.com/cyejing/news-hotspots)"
GITHUB_TRENDING_COOLDOWN_DEFAULT = 2.0


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global TIMEOUT, GITHUB_TRENDING_COOLDOWN_DEFAULT
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("github_trending", {})
    diagnostics_config = runtime.get("diagnostics", {})
    TIMEOUT = int(fetch_config.get("request_timeout_s", TIMEOUT) or TIMEOUT)
    GITHUB_TRENDING_COOLDOWN_DEFAULT = float(fetch_config.get("cooldown_s", GITHUB_TRENDING_COOLDOWN_DEFAULT) or 0)
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    设置日志配置。
    
    参数:
        verbose: 是否启用详细日志模式
        
    返回:
        配置好的 Logger 对象
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger(__name__)


def get_github_trending_cooldown_seconds() -> float:
    """Get sequential request cooldown for GitHub trending search."""
    return max(0.0, float(GITHUB_TRENDING_COOLDOWN_DEFAULT))


def parse_github_date(date_str: str) -> Optional[datetime]:
    """Parse GitHub API date format (ISO 8601 with Z suffix)."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        return None


def resolve_github_token() -> Optional[str]:
    """解析 GitHub token，仅支持 GITHUB_TOKEN 环境变量。"""
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        logging.debug("Using GITHUB_TOKEN from environment")
        return token

    logging.debug("No GitHub token found, using unauthenticated API (60 req/hour)")
    return None


def load_github_trending_queries(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, str]]:
    """
    从 topics.json 加载 GitHub Trending 查询配置。
    
    参数:
        defaults_dir: 默认配置目录
        config_dir: 用户配置目录（可选）
        
    返回:
        查询列表，每个元素包含 topic 和 q 字段
    """
    topics = load_topics_config(defaults_dir, config_dir)
    queries = []
    
    for topic in topics:
        if topic.get("id") != "github":
            continue
        search_config = topic.get("search", {})
        github_queries = search_config.get("github_queries") or []
        if isinstance(github_queries, list):
            for github_query in github_queries:
                if github_query:
                    queries.append({
                        "topic": "github",
                        "q": github_query,
                    })

    logging.info(f"从配置加载了 {len(queries)} 个 GitHub Trending 查询")
    return queries


def load_topics_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Load merged topics from defaults and optional workspace overrides."""
    sys.path.insert(0, str(Path(__file__).parent))
    from config_loader import load_merged_topics

    return load_merged_topics(defaults_dir, config_dir)


def fetch_trending_repos(hours: int = 48, github_token: Optional[str] = None,
                         min_stars: int = 50, per_topic: int = 15,
                         defaults_dir: Optional[Path] = None,
                         config_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    通过 GitHub Search API 抓取热门仓库。
    
    策略: 搜索在 `hours` 小时内推送的仓库，星标数 >= min_stars，按星标数降序排序。
    然后根据仓库年龄估算每日星标增长率。
    
    参数:
        hours: 回溯时间窗口（小时）
        github_token: GitHub API token（可选）
        min_stars: 最低星标数
        per_topic: 每个主题的最大仓库数
        defaults_dir: 默认配置目录
        config_dir: 用户配置目录（可选）
        
        返回:
        包含仓库列表与 query 统计的字典:
        - repos: 仓库列表
        - request_traces: 每次查询的请求耗时记录
        - queries_total: 查询总数
        - queries_ok: 成功查询数

        repos 中每个元素包含:
        - repo: 仓库全名 (owner/repo)
        - name: 仓库名称
        - description: 描述
        - url: GitHub URL
        - stars: 总星标数
        - daily_stars_est: 估算的每日星标增长
        - forks: Fork 数
        - language: 主要编程语言
        - topic: 单个主题标签
        - created_at: 创建时间
        - pushed_at: 最后推送时间
        - source_type: 数据源类型
    """
    effective_defaults_dir = defaults_dir or Path("config/defaults")
    trending_queries = load_github_trending_queries(effective_defaults_dir, config_dir)
    if not trending_queries:
        logging.warning("No GitHub trending queries configured under topic=github")
        return {
            "repos": [],
            "queries_total": 0,
            "queries_ok": 0,
            "request_traces": [],
            "failed_items": [],
        }
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github.v3+json",
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    all_repos = []
    seen_repos = set()
    request_traces: List[Dict[str, Any]] = []
    failed_items: List[Dict[str, Any]] = []
    cooldown_s = get_github_trending_cooldown_seconds()
    last_finished_at: Optional[float] = None
    logging.info("GitHub trending sequential cooldown: %.1fs", cooldown_s)

    for tq in trending_queries:
        if last_finished_at is not None and cooldown_s > 0:
            elapsed_since_last = time.time() - last_finished_at
            if elapsed_since_last < cooldown_s:
                time.sleep(cooldown_s - elapsed_since_last)

        q = f"{tq['q']} pushed:>{cutoff_str} stars:>{min_stars}"
        url = f"https://api.github.com/search/repositories?q={quote(q)}&sort=stars&order=desc&per_page={per_topic}"

        try:
            query_started_at = time.monotonic()
            req = Request(url, headers=headers)
            with urlopen(req, timeout=TIMEOUT) as resp:
                data = json.loads(resp.read().decode())

            for item in data.get("items", []):
                full_name = item["full_name"]
                if full_name in seen_repos:
                    continue
                seen_repos.add(full_name)

                # 估算每日星标增长
                created = parse_github_date(item.get("created_at", ""))
                age_days = max(1, (datetime.now(timezone.utc) - created).days) if created else 365
                stars = item.get("stargazers_count", 0)
                daily_stars = round(stars / age_days)

                all_repos.append({
                    "repo": full_name,
                    "name": item.get("name", ""),
                    "description": (item.get("description") or "")[:200],
                    "url": item.get("html_url", ""),
                    "stars": stars,
                    "daily_stars_est": daily_stars,
                    "forks": item.get("forks_count", 0),
                    "language": item.get("language", ""),
                    "topic": tq["topic"],
                    "created_at": item.get("created_at", ""),
                    "pushed_at": item.get("pushed_at", ""),
                    "source_type": "github_trending",
                })

            request_traces.append(build_request_trace(tq["q"], url, time.monotonic() - query_started_at, status="ok", topic=tq["topic"], backend="github-api"))
            logging.debug(f"Trending [{tq['topic']}]: {len(data.get('items', []))} repos")

        except HTTPError as e:
            failed_items.append(normalize_failed_item(tq["q"], f"HTTP {e.code}", 0))
            request_traces.append(build_request_trace(tq["q"], url, 0, status="error", error=f"HTTP {e.code}", topic=tq["topic"], backend="github-api"))
            logging.warning(f"GitHub trending search error [{tq['topic']}]: HTTP {e.code}")
        except Exception as e:
            failed_items.append(normalize_failed_item(tq["q"], str(e)[:180], 0))
            request_traces.append(build_request_trace(tq["q"], url, 0, status="error", error=str(e)[:180], topic=tq["topic"], backend="github-api"))
            logging.warning(f"GitHub trending search error [{tq['topic']}]: {e}")
        finally:
            last_finished_at = time.time()

    # 按星标数降序排序
    all_repos.sort(key=lambda x: -x["stars"])
    logging.info(f"🔥 Trending: {len(all_repos)} repos found across {len(trending_queries)} topics")
    return {
        "repos": all_repos,
        "queries_total": len(request_traces),
        "queries_ok": sum(1 for trace in request_traces if trace.get("status") == "ok"),
        "request_traces": request_traces,
        "failed_items": failed_items,
    }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch GitHub trending repos via Search API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-github-trending.py --hours 48 --output trending.json
    python3 fetch-github-trending.py --defaults config/defaults --config workspace/config
        """
    )
    parser.add_argument("--hours", type=int, default=48, help="Lookback window in hours (default: 48)")
    parser.add_argument("--min-stars", type=int, default=None, help="Minimum stars")
    parser.add_argument("--per-topic", type=int, default=None, help="Max repos per topic")
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default config directory")
    parser.add_argument("--config", type=Path, help="User config directory")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", action="store_true", help="Ignored (pipeline compat)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    runtime = apply_runtime_config(args.defaults, effective_config_dir)
    fetch_config = runtime.get("fetch", {}).get("github_trending", {})
    min_stars = args.min_stars if args.min_stars is not None else int(fetch_config.get("min_stars", 50) or 50)
    per_topic = args.per_topic if args.per_topic is not None else int(fetch_config.get("per_topic", 15) or 15)
    github_token = resolve_github_token()
    step_started_at = time.monotonic()
    trending_result = fetch_trending_repos(
        args.hours, github_token, min_stars, per_topic,
        defaults_dir=args.defaults,
        config_dir=effective_config_dir
    )
    repos = trending_result["repos"]

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "source_type": "github_trending",
        "articles": [
            {
                "title": f"{repo['repo']}: {repo['description']}" if repo.get("description") else repo["repo"],
                "link": repo.get("url", f"https://github.com/{repo['repo']}"),
                "date": repo.get("pushed_at", ""),
                "topic": str(repo.get("topic") or "github"),
                "source_type": "github_trending",
                "source_id": f"github-trending-{repo['repo']}",
                "source_name": "GitHub Trending",
                "source_priority": 4,
                "summary": repo.get("description", ""),
                "stars": repo.get("stars", 0),
                "daily_stars_est": repo.get("daily_stars_est", 0),
                "forks": repo.get("forks", 0),
                "language": repo.get("language", ""),
            }
            for repo in repos
        ],
    }

    out_path = args.output or Path(tempfile.mkstemp(prefix="news-hotspots-trending-", suffix=".json")[1])
    meta = build_step_meta(
        step_key="github_trending",
        status="ok" if trending_result["queries_ok"] == trending_result["queries_total"] and len(repos) > 0 else ("partial" if trending_result["queries_ok"] > 0 and len(repos) > 0 else "error"),
        elapsed_s=time.monotonic() - step_started_at,
        items=len(repos),
        calls_total=trending_result["queries_total"],
        calls_ok=trending_result["queries_ok"],
        failed_items=trending_result.get("failed_items", []),
        request_traces=trending_result.get("request_traces", []),
    )
    write_result_with_meta(out_path, output, meta)

    print(f"✅ {len(repos)} trending repos → {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
