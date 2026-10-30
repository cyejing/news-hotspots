#!/usr/bin/env python3
"""
Google News 抓取脚本。

职责：
- 读取 `topics.json` 中的 Google News 查询配置
- 逐个 query 调用 `bb-browser site google/news`
- 将搜索结果标准化为 `source_type=google` 的统一 `articles`
- 将失败请求、耗时和慢请求统计写入 `*.meta.json`

执行逻辑：
1. 加载 runtime 与 topics 配置
2. 逐个 topic、逐个 query 顺序请求 Google News
3. 成功结果立即标准化；失败请求跳过但保留失败明细
4. 输出结果 JSON 与 sidecar meta JSON

输出文件职责：
- `<step>.json`
  只保存抓到的 article 数据，给 `merge-sources.py` 合并
- `<step>.meta.json`
  只保存抓取诊断，不承担结果表达职责
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from config_loader import load_merged_runtime_config, load_merged_topics
    from step_contract import (
        build_request_trace,
        build_step_meta_from_traces,
        configure_slow_request_thresholds,
        from_timestamp_local,
        local_now,
        normalize_failed_item,
        write_result_with_meta,
    )
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from config_loader import load_merged_runtime_config, load_merged_topics
    from step_contract import (
        build_request_trace,
        build_step_meta_from_traces,
        configure_slow_request_thresholds,
        from_timestamp_local,
        local_now,
        normalize_failed_item,
        write_result_with_meta,
    )

COOLDOWN_SECONDS = 12.0
DEFAULT_TIMEOUT = 180
RESULTS_PER_QUERY = 10
# 主题相关度门槛（anti-SEO-spam）：宽泛查询词会命中垃圾站内容
# （实测 2026-10-30："Laptop-Friendly Cafes" 咖啡馆软文混入 technology 主题）。
# 规则：标题须命中 ≥1 个强特征词（专有名词/多词短语/CJK）或 ≥2 个
#普通词。模块默认关闭，生产由 runtime.json 的
# fetch.google.relevance_gate_enabled 开启。
RELEVANCE_GATE_ENABLED = False
_last_success_at: Optional[float] = None
_last_request_elapsed_s: Optional[float] = None


class TimedRuntimeError(RuntimeError):
    def __init__(self, message: str, elapsed_s: float, *, status: str = "error"):
        super().__init__(message)
        self.elapsed_s = elapsed_s
        self.status = str(status or "error")


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(__name__)


def throttle_after_success() -> None:
    global _last_success_at
    if _last_success_at is None:
        return
    wait_seconds = COOLDOWN_SECONDS - (time.monotonic() - _last_success_at)
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def apply_runtime_config(defaults_dir: Path, config_dir: Optional[Path] = None) -> Dict[str, Any]:
    global COOLDOWN_SECONDS, DEFAULT_TIMEOUT, RESULTS_PER_QUERY, RELEVANCE_GATE_ENABLED
    runtime = load_merged_runtime_config(defaults_dir, config_dir)
    fetch_config = runtime.get("fetch", {}).get("google", {})
    diagnostics_config = runtime.get("diagnostics", {})
    COOLDOWN_SECONDS = float(fetch_config.get("cooldown_s", COOLDOWN_SECONDS) or 0)
    DEFAULT_TIMEOUT = int(fetch_config.get("request_timeout_s", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    RESULTS_PER_QUERY = int(fetch_config.get("results_per_query", RESULTS_PER_QUERY) or RESULTS_PER_QUERY)
    RELEVANCE_GATE_ENABLED = bool(fetch_config.get("relevance_gate_enabled", RELEVANCE_GATE_ENABLED))
    configure_slow_request_thresholds(diagnostics_config.get("slow_request_thresholds_s", []))
    return runtime


def clear_last_request_elapsed() -> None:
    global _last_request_elapsed_s
    _last_request_elapsed_s = None


def last_request_elapsed(default: float = 0.0) -> float:
    return float(_last_request_elapsed_s if _last_request_elapsed_s is not None else default)


def run_bb_browser_site(args: Sequence[str], timeout: Optional[int] = None) -> Dict[str, Any]:
    global _last_success_at, _last_request_elapsed_s
    throttle_after_success()
    effective_timeout = int(timeout if timeout is not None else DEFAULT_TIMEOUT)
    request_started_at = time.monotonic()
    try:
        result = subprocess.run(
            ["bb-browser", "site", *args],
            capture_output=True,
            text=True,
            timeout=effective_timeout,
            env=os.environ,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed_s = time.monotonic() - request_started_at
        _last_request_elapsed_s = elapsed_s
        raise TimedRuntimeError(f"timed out after {effective_timeout} seconds", elapsed_s, status="timeout") from exc
    elapsed_s = time.monotonic() - request_started_at
    _last_request_elapsed_s = elapsed_s
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "bb-browser command failed").strip()
        raise TimedRuntimeError(message, elapsed_s)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TimedRuntimeError(f"Invalid JSON from bb-browser: {exc}", elapsed_s) from exc
    _last_success_at = time.monotonic()
    return payload


def normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def result_count_for_topic(topic: Dict[str, Any]) -> int:
    return max(1, RESULTS_PER_QUERY)


def format_google_term(term: str, exclude: bool = False) -> str:
    value = normalize_text(term)
    if not value:
        return ""
    if " " in value:
        value = f"\"{value}\""
    return f"-{value}" if exclude else value


def build_google_query(base_query: str, exclude: List[str]) -> str:
    parts = [normalize_text(base_query)]
    parts.extend(
        formatted
        for formatted in (format_google_term(term, exclude=True) for term in exclude)
        if formatted
    )
    return " ".join(part for part in parts if part)


# 主题相关度门槛的核心词表：从编译后查询词抽取强/普通特征词。
# 强特征：引号短语、专有名词（合驼峰/全大写）、≥7 字符长词、CJK；
# 普通词：其余 ≥3 字符词（整词匹配）；<3 字符忽略（"ai"/"us" 太泛）。
# 停用词在分词阶段过滤。
_GATE_STOPWORDS = frozenset(
    (
        "or and the a an of for with new in on to from by at is are be as that this it its "
        "will would could should may might can not no but if then than when where what how why "
        "study says report reports latest news update updates "
        "vs guide best top trends trend "
        "launch release releases announce announces announced "
        "use uses used using make makes made get gets "
        "about into over under after before during between "
        "more most less least very much many few "
        "one two three first second next last "
        "all any some each every other another "
        "have has had do does did done "
    ).split()
)


def _strip_exclude_terms(query: str) -> str:
    """去掉 -word / -"phrase" 排除项，只留正向词。"""
    query = re.sub(r'-"[^"]+"', " ", query)
    query = re.sub(r"(?<![\w-])-\w+", " ", query)
    return query


def _extract_gate_terms(query: str) -> tuple:
    """从查询词抽取 (强特征词集合, 普通词集合)。"""
    strong: set = set()
    plain: set = set()
    positive = _strip_exclude_terms(query)
    # 引号短语 → 强特征（保留内部空格，匹配时子串印射）
    for phrase in re.findall(r'"([^"]+)"', positive):
        cleaned = " ".join(phrase.lower().split())
        if cleaned and cleaned not in _GATE_STOPWORDS:
            strong.add(cleaned)
    quoted_spans = re.findall(r'"[^"]+"', positive)
    bare = positive
    for span in quoted_spans:
        bare = bare.replace(span, " ")
    for raw in re.split(r"[\s()]+", bare):
        token = raw.strip('"-').strip()
        if not token or token.upper() in ("OR", "AND"):
            continue
        lower = token.lower()
        if lower in _GATE_STOPWORDS:
            continue
        if any("\u4e00" <= ch <= "\u9fff" for ch in token):
            strong.add(lower)
            continue
        has_internal_caps = any(ch.isupper() for ch in token[1:])
        if has_internal_caps or (token[:1].isupper() and len(token) >= 3):
            strong.add(lower)  # OpenAI / TSMC / AMD / Nvidia 等专有名词
            continue
        if len(lower) >= 7:
            strong.add(lower)  # semiconductor / earnings 等长术语
        elif len(lower) >= 3:
            plain.add(lower)
    return strong, plain


def _title_passes_gate(title: str, strong: set, plain: set) -> bool:
    """标题命中 ≥1 强特征词或 ≥2 普通词才故行。"""
    if not strong and not plain:
        return True  # 无可用特征词时不过滤（保守）
    title_lower = title.lower()
    if any(term in title_lower for term in strong):
        return True
    hits = 0
    for term in plain:
        if re.search(r"\b" + re.escape(term) + r"\b", title_lower):
            hits += 1
            if hits >= 2:
                return True
    return False


def passes_relevance_gate(title: str, compiled_query: str) -> bool:
    """对外入口：判断 google 结果标题是否与查询主题相关。"""
    if not RELEVANCE_GATE_ENABLED:
        return True
    strong, plain = _extract_gate_terms(compiled_query)
    return _title_passes_gate(title, strong, plain)


def fetch_topic(topic: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    search = topic.get("search", {})
    queries = list(search.get("google_queries", []))
    exclude = search.get("exclude", [])
    per_query = result_count_for_topic(topic)

    dedup_by_url: Dict[str, Dict[str, Any]] = {}
    request_traces: List[Dict[str, Any]] = []
    total_request_elapsed_s = 0.0
    ok_queries = 0
    marked_irrelevant = 0

    for query in queries:
        compiled_query = build_google_query(query, exclude)
        try:
            clear_last_request_elapsed()
            payload = run_bb_browser_site(["google/news", compiled_query, str(per_query)])
            elapsed_s = last_request_elapsed()
            results = payload.get("results", [])
            kept = 0
            for item in results:
                article = {
                    "title": normalize_text(item.get("title", "")),
                    "link": item.get("url", ""),
                    "snippet": normalize_text(item.get("snippet", "")),
                    "summary": normalize_text(item.get("snippet", "")),
                    "date": from_timestamp_local(item.get("timestamp", time.time())).isoformat(),
                    "topic": topic.get("id"),
                    "publisher": normalize_text(item.get("source", "")),
                    "source_name": normalize_text(item.get("source", "")) or "Google News",
                    "google_query": compiled_query,
                }
                if not article["title"] or not article["link"]:
                    continue
                # 相关度门槛：不硬丢弃（Google 全文匹配，正经新闻标题也常不含查询词），
                # 未过关的打标记，由 merge-sources 扣分降权
                gate_text = article["title"] + " " + (article["summary"] or "")
                if not passes_relevance_gate(gate_text, compiled_query):
                    article["relevance_gate_pass"] = False
                    marked_irrelevant += 1
                else:
                    article["relevance_gate_pass"] = True
                dedup_by_url.setdefault(article["link"], article)
                kept += 1
            total_request_elapsed_s += elapsed_s
            request_traces.append(
                build_request_trace(
                    topic.get("id") or compiled_query,
                    compiled_query,
                    elapsed_s,
                    status="ok",
                    source_type="google",
                    method="CLI",
                    attempt=1,
                    backend="bb-browser",
                    adapter="google/news",
                )
            )
            ok_queries += 1
        except Exception as exc:
            logger.warning("Google News query failed [%s]: %s", topic.get("id"), exc)
            elapsed_s = getattr(exc, "elapsed_s", last_request_elapsed())
            total_request_elapsed_s += elapsed_s
            request_traces.append(
                build_request_trace(
                    topic.get("id") or compiled_query,
                    compiled_query,
                    elapsed_s,
                    status=getattr(exc, "status", "error"),
                    source_type="google",
                    method="CLI",
                    attempt=1,
                    backend="bb-browser",
                    adapter="google/news",
                    error=str(exc)[:200],
                )
            )

    articles = list(dedup_by_url.values())
    articles.sort(key=lambda article: article.get("date", ""), reverse=True)
    return {
        "topic_id": topic.get("id"),
        "status": "ok" if articles else "error",
        "elapsed_s": round(total_request_elapsed_s, 3),
        "calls_total": len(queries),
        "calls_ok": ok_queries,
        "failed_items": [],
        "request_traces": request_traces,
        "items": len(articles),
        "count": len(articles),
        "articles": articles,
        "marked_irrelevant": marked_irrelevant,
    }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Google News results via bb-browser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 fetch-google.py --defaults config/defaults --config workspace/config --output google.json
    python3 fetch-google.py --output google.json --verbose
        """,
    )
    parser.add_argument("--defaults", type=Path, default=Path("config/defaults"), help="Default configuration directory")
    parser.add_argument("--config", type=Path, help="User configuration directory for overlays")
    parser.add_argument("--output", "-o", type=Path, help="Output JSON path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--hours", type=int, default=48, help="Accepted for CLI consistency; not used by Google News fetch")
    parser.add_argument("--force", action="store_true", help="Accepted for CLI consistency; this fetcher always refreshes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    logger = setup_logging(args.verbose)
    effective_config_dir = args.config if args.config and args.config.exists() else None
    apply_runtime_config(args.defaults, effective_config_dir)
    if not args.output:
        fd, temp_path = tempfile.mkstemp(prefix="news-hotspots-google-", suffix=".json")
        os.close(fd)
        args.output = Path(temp_path)

    try:
        topics = load_merged_topics(args.defaults, effective_config_dir)
        step_started_at = time.monotonic()
        logger.info("Fetching Google News for %d topics sequentially", len(topics))
        logger.info("Google bb-browser cooldown: %.1fs | relevance gate: %s", COOLDOWN_SECONDS, "on" if RELEVANCE_GATE_ENABLED else "off")
        topic_results = [fetch_topic(topic, logger) for topic in topics if topic.get("search", {}).get("google_queries")]
        total_marked_irrelevant = sum(int(result.get("marked_irrelevant", 0) or 0) for result in topic_results)
        if total_marked_irrelevant:
            logger.info("🧹 Relevance gate marked %d off-topic results for down-weighting", total_marked_irrelevant)
        ok_topics = sum(1 for result in topic_results if result["status"] == "ok")
        total_articles = sum(result.get("count", 0) for result in topic_results)
        total_queries = sum(int(result.get("calls_total", 0) or 0) for result in topic_results)
        ok_queries = sum(int(result.get("calls_ok", 0) or 0) for result in topic_results)
        articles = [article for result in topic_results for article in result.get("articles", []) if isinstance(article, dict)]
        request_traces = [trace for result in topic_results for trace in result.get("request_traces", []) if isinstance(trace, dict)]
        output = {
            "generated": local_now().isoformat(),
            "source_type": "google",
            "articles": articles,
        }
        meta = build_step_meta_from_traces(
            step_key="google",
            status="ok" if ok_queries == total_queries and total_articles > 0 else ("partial" if ok_queries > 0 and total_articles > 0 else "error"),
            elapsed_total_s=round(time.monotonic() - step_started_at, 3),
            items=total_articles,
            calls_total=total_queries,
            calls_ok=ok_queries,
            failed_items=None,
            request_traces=request_traces,
        )
        write_result_with_meta(args.output, output, meta)
        status = meta["status"]
        failed_queries = max(0, total_queries - ok_queries)
        log_message = "%s Done: %d/%d topics ok, %d/%d queries ok, %d failed, %d articles → %s"
        log_args = (
            "✅" if status == "ok" else ("⚠️" if status == "partial" else "❌"),
            ok_topics,
            len(topic_results),
            ok_queries,
            total_queries,
            failed_queries,
            total_articles,
            args.output,
        )
        if status == "partial":
            logger.warning(log_message, *log_args)
        elif status == "ok":
            logger.info(log_message, *log_args)
        else:
            logger.error(log_message, *log_args)
        return 0 if total_articles > 0 else 1
    except Exception as exc:
        logger.error("💥 Google fetch failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
