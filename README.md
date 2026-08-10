# News Hotspots

> Automated global tech and AI hotspots built around a single pipeline and a script-generated final hotspots workflow.

**English** | [中文](README_CN.md)

[![Tests](https://github.com/cyejing/news-hotspots/actions/workflows/test.yml/badge.svg)](https://github.com/cyejing/news-hotspots/actions/workflows/test.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![ClawHub](https://img.shields.io/badge/ClawHub-news--hotspots-blueviolet)](https://clawhub.com/cyejing/news-hotspots)
[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Install in One Message

Tell your [OpenClaw](https://openclaw.ai) assistant any of:

> **"Generate today's news hotspots"** — run the pipeline and deliver a summary now.
> **"Set up a daily hotspots task every morning at 9am"** — schedule a recurring cron task.
> **"Give me another batch of hotspots"** — re-run the fetch and deliver a fresh summary.
> **"How is the fetch health today?"** — run diagnostics and interpret the report.

The bot handles installation, configuration, scheduling, fetching, and delivery.

Or install via CLI:

```bash
clawhub install news-hotspots
```

## What You Get

A quality-scored, deduplicated hotspots feed built from a single pipeline entrypoint:

| Layer | Sources | What |
|-------|---------|------|
| RSS | Config-driven feeds | OpenAI, NVIDIA, TechCrunch, BBC, Fed, 36氪 |
| Social / Search | Twitter, Google News | High-signal social and search-driven discovery |
| API Sources | Multiple endpoints | Weibo Hot Search, WallStreetCN, Tencent News, Hacker News |

### Pipeline

```text
       run-pipeline.py
              ↓
  RSS ────────┐
  Twitter ────┤── parallel fetch ──→ merge-sources.py
  Google ─────┤
  API ────────┤
  Zhihu ──────┤
  Weibo ──────┤
  Toutiao ────┘                          ↓
              Quality Scoring → Dedup → Topic Grouping
                             ↓
          merge-hotspots.py → archived JSON + Markdown
```

**Quality scoring**: layered scoring with source priority as a light base signal, fetch-local ranking, capped cross-source hotness bonus, recency bonus, and a strong history-similarity penalty. Topic output is then diversity-reranked so one fetch type does not dominate the top of a section. Each topic surfaces up to **10** items by default (`pipeline.default_hotspots_top_n`). Short or low-information titles (e.g. bare `@mentions`, single-word tweets) are hard-filtered before scoring.

The intended flow is:
- run `scripts/run-pipeline.py` once
- let `merge-hotspots.py` generate and archive the final hotspots JSON and Markdown
- deliver a **fetch summary + topic-structured digest + hotspot summary** (the archived Markdown is not echoed back item-by-item)

Archived files:
- `merge-hotspots.py` generates the final JSON and Markdown outputs
- `run-pipeline.py` archives the `meta` diagnostic files

For diagnostics:
- current-run health comes from step metadata in the debug directory
- 7-day history comes from archived `meta/` files under `workspace/archive/news-hotspots/<DATE>/`

## Configuration

- `config/defaults/rss.json` / `twitter.json` — split built-in source configs by fetch type
- `config/defaults/api.json` — split built-in API sources config
- `config/defaults/topics.json` — topics with search queries and display settings
- `config/defaults/runtime.json` — runtime defaults for fetch timeouts, cooldowns, retries, limits, diagnostics, and pipeline settings
- User overrides in `workspace/config/` take priority

Default topic set includes `ai-frontier`, `ai-infra`, `twitter`, `technology`, `business`, `value-investing`, `world`, `science`, and `social`.

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

## Runtime Configuration

Timeout and cooldown settings are configured through `runtime.json` (see `config/defaults/runtime.json`); there are no required runtime environment variables. RSS fetching honors the standard `ALL_PROXY` / `HTTPS_PROXY` / `HTTP_PROXY` environment variables (including `socks5h://`) when set.

## Dependencies

Python 3.8+ with:

```bash
pip install feedparser>=6.0.0 jsonschema>=4.0.0 requests>=2.28.0 pysocks>=1.7.1 beautifulsoup4>=4.12.0 rapidfuzz>=3.0.0
```

## Output

- `merge-hotspots.py` produces the final archived hotspots JSON and Markdown outputs.
- The final user-facing delivery is a **fetch summary + topic-structured digest + hotspot summary** (source counts, totals, generation time, then topic-grouped bullets of the form `- <summary>:<content>` — each topic merges similar items, drops noise, and keeps all remaining items with no count cap; cross-source resonance is marked inline with `🔥` on the bullet rather than a separate block; after all topics, a single hotspot summary paragraph wraps up the main narrative, trend direction, and cross-source signals). The archived Markdown is kept on disk for reference, not echoed back item-by-item.
- JSON hotspots are archived in `workspace/archive/news-hotspots/<DATE>/json/`.
- Markdown is archived in `workspace/archive/news-hotspots/<DATE>/markdown/`.
- Archived filenames use the selected mode, for example `daily.json`, `daily.md`, `weekly.json`, `weekly1.md`.
- Step diagnostics are archived in `workspace/archive/news-hotspots/<DATE>/meta/`.
- If you want delivery to other systems, consume the generated Markdown archive externally.

## Diagnostics

`source-health.py` reads only `meta/*.meta.json` (never re-reads fetch result JSON) and analyzes:

- per-source status, call stats, and failure items
- **log pattern classification** over `logs.tail` and `failed_items[].error`: timeout / rate-limited / connection / parse / empty
- **slow request rate** from `slow_requests` over total calls
- **items trend** (median across recent runs) and **consecutive degraded days** per source

Current run:

```bash
uv run scripts/source-health.py --defaults config/defaults --config workspace/config --input workspace/archive/news-hotspots/<DATE>/meta --verbose
```

Recent history (aggregates the last `diagnostics.history_days`, default 7):

```bash
uv run scripts/source-health.py --defaults config/defaults --config workspace/config --input workspace/archive/news-hotspots --verbose
```

## Repository

**GitHub**: [github.com/cyejing/news-hotspots](https://github.com/cyejing/news-hotspots)

## License

MIT License — see [LICENSE](LICENSE) for details.
