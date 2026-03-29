# News Hotspots

> Automated global tech and AI hotspots built around a single pipeline and `hotspots.json`-first workflow.

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
                    hotspots.json → Markdown report
```

**Quality scoring**: layered scoring with source priority as a light base signal, fetch-local ranking, capped cross-source hotness bonus, recency bonus, and a strong history-similarity penalty. Topic output is then diversity-reranked so one fetch type does not dominate the top of a section.

The intended flow is:
- run `scripts/run-pipeline.py` once
- read only the generated `hotspots.json`
- write the final Markdown hotspots report from that file

For diagnostics:
- current-run health comes from step metadata in the debug directory
- 7-day history comes from archived `meta/` files under `workspace/archive/news-hotspots/<DATE>/`

## Configuration

- `config/defaults/sources.json` — 148 built-in config sources grouped by `type`
- `config/defaults/topics.json` — 10 topics with search queries and display settings
- User overrides in `workspace/config/` take priority

Default topic set includes `ai-models`, `ai-agents`, `ai-ecosystem`, `technology`, `developer-tools`, `github`, `markets-business`, `macro-policy`, `world-affairs`, and `cybersecurity`.

RSS defaults are intentionally conservative: obvious personal blogs stay in `sources.rss` as candidate entries at the end of the list with `"enabled": false`.

Priority guideline:
- `3` is the default for most sources.
- `4` is for clearly authoritative or consistently high-signal sources.
- `5` is reserved for a small number of top-tier primary sources.
- Avoid raising many sources at once; let cross-source confirmation and freshness do most of the ranking work.

## Customize Your Sources

```bash
cp config/defaults/sources.json workspace/config/news-hotspots-sources.json
cp config/defaults/topics.json workspace/config/news-hotspots-topics.json
```

Overlay files merge with defaults. Matching `id` overrides a source, a new `id` appends a source, and `"enabled": false` disables a built-in source.

## Environment Variables

```bash
export GITHUB_TOKEN="..."
```

## Dependencies

Python 3.8+ with:

```bash
pip install feedparser>=6.0.0 jsonschema>=4.0.0 requests>=2.28.0 beautifulsoup4>=4.12.0 rapidfuzz>=3.0.0
```

## Output

- The pipeline produces `hotspots.json` as the only intended LLM input.
- The final user-facing output is a Markdown hotspots report.
- JSON hotspots are archived in `workspace/archive/news-hotspots/<DATE>/json/`.
- User-facing Markdown is archived in `workspace/archive/news-hotspots/<DATE>/markdown/`.
- Step diagnostics are archived in `workspace/archive/news-hotspots/<DATE>/meta/`.
- If you want delivery to other systems, consume the generated Markdown archive externally.

## Diagnostics

Current run:

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-hotspots/<DATE> --verbose
```

Recent history:

```bash
uv run scripts/source-health.py --input-dir workspace/archive/news-hotspots --verbose
```

## Repository

**GitHub**: [github.com/cyejing/news-hotspots](https://github.com/cyejing/news-hotspots)

## License

MIT License — see [LICENSE](LICENSE) for details.
