#!/usr/bin/env python3
"""
Shared step/source_type registry for news-hotspots.

This module centralizes the canonical fetch step list so pipeline orchestration,
merge input loading, and source_type ordering all derive from the same registry.
"""

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple


@dataclass(frozen=True)
class StepRegistration:
    step_key: str
    source_type: str
    display_name: str
    script_name: str
    merge_arg: str
    enabled_by_default: bool = True


ALL_SOURCE_STEPS: Tuple[StepRegistration, ...] = (
    StepRegistration("rss", "rss", "RSS", "fetch-rss.py", "--rss"),
    StepRegistration("twitter", "twitter", "Twitter", "fetch-twitter.py", "--twitter"),
    StepRegistration("google", "google", "Google News", "fetch-google.py", "--google"),
    StepRegistration("github", "github", "GitHub", "fetch-github.py", "--github"),
    StepRegistration("github_trending", "github_trending", "GitHub Trending", "fetch-github-trending.py", "--github-trending"),
    StepRegistration("api", "api", "API", "fetch-api.py", "--api"),
    StepRegistration("v2ex", "v2ex", "V2EX", "fetch-v2ex.py", "--v2ex"),
    StepRegistration("zhihu", "zhihu", "Zhihu", "fetch-zhihu.py", "--zhihu"),
    StepRegistration("weibo", "weibo", "Weibo", "fetch-weibo.py", "--weibo"),
    StepRegistration("toutiao", "toutiao", "Toutiao", "fetch-toutiao.py", "--toutiao"),
    StepRegistration("reddit", "reddit", "Reddit", "fetch-reddit.py", "--reddit"),
)

STEP_KEYS: Tuple[str, ...] = tuple(step.step_key for step in ALL_SOURCE_STEPS)
STEP_BY_KEY: Dict[str, StepRegistration] = {step.step_key: step for step in ALL_SOURCE_STEPS}


def iter_fetch_steps() -> Iterable[StepRegistration]:
    return ALL_SOURCE_STEPS
