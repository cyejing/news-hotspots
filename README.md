# News Hotspots

> Automated global tech and AI hotspots built around a single pipeline and a script-generated final hotspots workflow.

**English** | [中文](README_CN.md)

[![Tests](https://github.com/cyejing/news-hotspots/actions/workflows/test.yml/badge.svg)](https://github.com/cyejing/news-hotspots/actions/workflows/test.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![ClawHub](https://img.shields.io/badge/ClawHub-news--hotspots-blueviolet)](https://clawhub.com/cyejing/news-hotspots)
[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Install in One Message

Tell your [OpenClaw](https://openclaw.ai) assistant:

> **"Install news-hotspots and generate a daily markdown hotspots report every morning at 9am"**

The bot handles installation, configuration, scheduling, and archive generation.

Or install via CLI:

```bash
clawhub install news-hotspots
```

## What You Get

A quality-scored, deduplicated hotspots feed built from a single pipeline entrypoint:

| Layer | Sources | What |
|-------|---------|------|
| RSS | Config-driven feeds | OpenAI, NVIDIA, TechCrunch, BBC, Fed, 36氪 |
| GitHub | Releases + Trending | Key repos and GitHub Trending, grouped under the `github` topic |
| Social / Search | Twitter, Reddit, Google News, V2EX | High-signal community and search-driven discovery |
| API Sources | Multiple endpoints | Weibo Hot Search, WallStreetCN, Tencent News, Hacker News |

### Pipeline

```text
       run-pipeline.py
              ↓
  RSS ────────┐
  GitHub ─────┤── parallel fetch ──→ merge-sources.py
  GitHub Tr. ─┤
  API ────────┤
  Twitter ────┤
  Reddit ─────┤
  Google ─────┤
  V2EX ───────┘                          ↓
              Quality Scoring → Dedup → Topic Grouping
                             ↓
          merge-hotspots.py → archived JSON + Markdown
```

**Quality scoring**: layered scoring with source priority as a light base signal, fetch-local ranking, capped cross-source hotness bonus, recency bonus, and a strong history-similarity penalty. Topic output is then diversity-reranked so one fetch type does not dominate the top of a section.

The intended flow is:
- run `scripts/run-pipeline.py` once
- let `merge-hotspots.py` generate and archive the final hotspots JSON and Markdown
- read the archived Markdown for delivery

Archived files:
- `merge-hotspots.py` generates the final JSON and Markdown outputs
- `run-pipeline.py` archives the `meta` diagnostic files

For diagnostics:
- current-run health comes from step metadata in the debug directory
- 7-day history comes from archived `meta/` files under `workspace/archive/news-hotspots/<DATE>/`

## Configuration

- `config/defaults/rss.json` / `twitter.json` / `github.json` / `reddit.json` — split built-in source configs by fetch type
- `config/defaults/api.json` — split built-in API sources config
- `config/defaults/topics.json` — 10 topics with search queries and display settings
- `config/defaults/runtime.json` — runtime defaults for fetch timeouts, cooldowns, retries, limits, diagnostics, and pipeline settings
- User overrides in `workspace/config/` take priority

Default topic set includes `ai-frontier`, `ai-infra`, `github`, `technology`, `business`, `world`, `science`, and `social`.

RSS defaults are intentionally conservative: obvious personal blogs stay in `rss.json` as candidate entries at the end of the list with `"enabled": false`.

Priority guideline:
- `3` is the default for most sources.
- `4` is for clearly authoritative or consistently high-signal sources.
- `5` is reserved for a small number of top-tier primary sources.
- Avoid raising many sources at once; let cross-source confirmation and freshness do most of the ranking work.

## Customize Your Sources

```bash
cp config/defaults/rss.json workspace/config/news-hotspots-rss.json
cp config/defaults/twitter.json workspace/config/news-hotspots-twitter.json
cp config/defaults/github.json workspace/config/news-hotspots-github.json
cp config/defaults/reddit.json workspace/config/news-hotspots-reddit.json
cp config/defaults/topics.json workspace/config/news-hotspots-topics.json
```

Overlay files merge with defaults. Matching `id` overrides a source, a new `id` appends a source, and `"enabled": false` disables a built-in source.

## Runtime Overrides

```bash
cp config/defaults/runtime.json workspace/config/news-hotspots-runtime.json
```

Runtime config uses deep merge:
- CLI flags override runtime defaults
- `workspace/config/news-hotspots-runtime.json` overrides `config/defaults/runtime.json`
- unspecified sibling fields keep their default values

Use runtime config for:
- fetch timeouts, cooldowns, retries, concurrency, and limits
- pipeline step timeout, merge timeout, hotspots timeout, archive retention, and default top N
- diagnostics thresholds used by `source-health.py` and `step_contract.py`

## Environment Variables

```bash
export GITHUB_TOKEN="..."
```

`GITHUB_TOKEN` is the only remaining runtime environment variable. Timeout and cooldown settings are configured through `runtime.json`.

## Dependencies

Python 3.8+ with:

```bash
pip install feedparser>=6.0.0 jsonschema>=4.0.0 requests>=2.28.0 beautifulsoup4>=4.12.0 rapidfuzz>=3.0.0
```

## Output

- `merge-hotspots.py` produces the final archived hotspots JSON and Markdown outputs.
- The final user-facing output is a Markdown hotspots report.
- The final user-facing Markdown includes the translated body plus a closing AI summary for both daily and weekly reports.
- JSON hotspots are archived in `workspace/archive/news-hotspots/<DATE>/json/`.
- User-facing Markdown is archived in `workspace/archive/news-hotspots/<DATE>/markdown/`.
- Archived filenames use the selected mode, for example `daily.json`, `daily.md`, `weekly.json`, `weekly1.md`.
- Step diagnostics are archived in `workspace/archive/news-hotspots/<DATE>/meta/`.
- If you want delivery to other systems, consume the generated Markdown archive externally.

## Diagnostics

Current run:

```bash
uv run scripts/source-health.py --input workspace/archive/news-hotspots/<DATE>/meta --verbose
```

Recent history:

```bash
uv run scripts/source-health.py --input workspace/archive/news-hotspots --verbose
```

## Repository

**GitHub**: [github.com/cyejing/news-hotspots](https://github.com/cyejing/news-hotspots)

## License

MIT License — see [LICENSE](LICENSE) for details.
