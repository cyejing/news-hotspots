#!/usr/bin/env python3
"""
GitHub Trending 仓库抓取脚本 - 用于 news-digest 技能。

通过 GitHub Search API 抓取热门仓库，支持从 topics.json 配置文件加载查询。
按星标数排序，估算每日星标增长率。

核心功能：
- 从 topics.json 加载 GitHub Trending 查询配置
- 使用 GitHub Search API 搜索热门仓库
- 按时间窗口和最低星标数过滤
- 估算每日星标增长率
- 支持多种认证方式（GitHub Token、GitHub App）

使用方法:
    python3 fetch-github-trending.py \
        --defaults config/defaults \
        --config workspace/config \
        --hours 48 \
        --output trending.json \
        --verbose

环境变量:
    GITHUB_TOKEN - GitHub 个人访问令牌（可选，提高速率限制）
    GH_APP_ID - GitHub App ID（可选）
    GH_APP_INSTALL_ID - GitHub App Installation ID（可选）
    GH_APP_KEY_FILE - GitHub App 私钥文件路径（可选）
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

# ==================== 常量配置 ====================
TIMEOUT = 60  # 请求超时时间（秒）
USER_AGENT = "NewsDigest/3.0 (bot; +https://github.com/cyejing/news-digest)"
GITHUB_TRENDING_COOLDOWN_ENV = "NEWS_DIGEST_GITHUB_TRENDING_COOLDOWN_SECONDS"
GITHUB_TRENDING_COOLDOWN_DEFAULT = 2.0


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
    raw = os.environ.get(
        GITHUB_TRENDING_COOLDOWN_ENV,
        str(GITHUB_TRENDING_COOLDOWN_DEFAULT),
    )
    try:
        return max(0.0, float(raw))
    except ValueError:
        return GITHUB_TRENDING_COOLDOWN_DEFAULT


def parse_github_date(date_str: str) -> Optional[datetime]:
    """Parse GitHub API date format (ISO 8601 with Z suffix)."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        return None


def resolve_github_token() -> Optional[str]:
    """
    解析 GitHub token，支持多种认证方式。
    
    优先级:
        1. GITHUB_TOKEN 环境变量
        2. GitHub App 自动生成 token
        3. gh CLI token
        4. None (未认证，60 请求/小时)
        
    返回:
        GitHub token 或 None
    """
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        logging.debug("Using GITHUB_TOKEN from environment")
        return token
    
    # Try GitHub App token generation
    app_id = os.environ.get("GH_APP_ID")
    install_id = os.environ.get("GH_APP_INSTALL_ID")
    key_file = os.environ.get("GH_APP_KEY_FILE")
    
    if app_id and install_id and key_file:
        try:
            import subprocess
            # Generate JWT
            now = int(time.time())
            payload = f'{{"iat":{now-60},"exp":{now+540},"iss":{app_id}}}'
            
            jwt_result = subprocess.run(
                ["openssl", "dgst", "-sha256", "-sign", key_file],
                input=payload.encode(),
                capture_output=True,
                timeout=10
            )
            
            if jwt_result.returncode == 0:
                # Get installation token
                jwt_token = jwt_result.stdout.hex()
                req = Request(
                    f"https://api.github.com/app/installations/{install_id}/access_tokens",
                    headers={
                        "Authorization": f"Bearer {jwt_token}",
                        "Accept": "application/vnd.github.v3+json",
                        "User-Agent": USER_AGENT,
                    },
                    method="POST"
                )
                with urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())
                    token = data.get("token")
                    if token:
                        logging.debug("Generated GitHub App installation token")
                        return token
        except Exception as e:
            logging.debug(f"GitHub App token generation failed: {e}")
    
    # Try gh CLI
    try:
        import subprocess
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            token = result.stdout.strip()
            if token:
                logging.debug("Using token from gh CLI")
                return token
    except Exception:
        pass
    
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

        if not queries:
            github_query = search_config.get("github_query")
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
                         config_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
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
        仓库列表，每个元素包含:
        - repo: 仓库全名 (owner/repo)
        - name: 仓库名称
        - description: 描述
        - url: GitHub URL
        - stars: 总星标数
        - daily_stars_est: 估算的每日星标增长
        - forks: Fork 数
        - language: 主要编程语言
        - topics: 主题标签列表
        - created_at: 创建时间
        - pushed_at: 最后推送时间
        - source_type: 数据源类型
    """
    # 从配置文件加载查询
    if defaults_dir:
        trending_queries = load_github_trending_queries(defaults_dir, config_dir)
    else:
        # 回退到硬编码查询（向后兼容）
        trending_queries = [
            {"topic": "github", "q": "llm large-language-model in:topics,name,description"},
            {"topic": "github", "q": "ai-agent autonomous-agent in:topics,name,description"},
            {"topic": "github", "q": "machine-learning deep-learning in:topics,name,description"},
            {"topic": "github", "q": "developer programming in:topics,name,description"},
            {"topic": "github", "q": "finance trading in:topics,name,description"},
            {"topic": "github", "q": "security cybersecurity in:topics,name,description"},
        ]
    
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
                    "topics": [tq["topic"]],
                    "created_at": item.get("created_at", ""),
                    "pushed_at": item.get("pushed_at", ""),
                    "source_type": "github_trending",
                })

            logging.debug(f"Trending [{tq['topic']}]: {len(data.get('items', []))} repos")

        except HTTPError as e:
            logging.warning(f"GitHub trending search error [{tq['topic']}]: HTTP {e.code}")
        except Exception as e:
            logging.warning(f"GitHub trending search error [{tq['topic']}]: {e}")
        finally:
            last_finished_at = time.time()

    # 按星标数降序排序
    all_repos.sort(key=lambda x: -x["stars"])
    logging.info(f"🔥 Trending: {len(all_repos)} repos found across {len(trending_queries)} topics")
    return all_repos


def main():
    """CLI 入口函数。"""
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
    parser.add_argument("--min-stars", type=int, default=50, help="Minimum stars (default: 50)")
    parser.add_argument("--per-topic", type=int, default=15, help="Max repos per topic (default: 15)")
    parser.add_argument("--defaults", type=Path, help="Default config directory")
    parser.add_argument("--config", type=Path, help="User config directory")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", action="store_true", help="Ignored (pipeline compat)")
    args = parser.parse_args()

    setup_logging(args.verbose)
    github_token = resolve_github_token()
    repos = fetch_trending_repos(
        args.hours, github_token, args.min_stars, args.per_topic,
        defaults_dir=args.defaults,
        config_dir=args.config
    )

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "source_type": "github_trending",
        "hours": args.hours,
        "min_stars": args.min_stars,
        "cooldown_s": get_github_trending_cooldown_seconds(),
        "total": len(repos),
        "repos": repos,
    }

    out_path = args.output or Path(tempfile.mkstemp(prefix="news-digest-trending-", suffix=".json")[1])
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ {len(repos)} trending repos → {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
