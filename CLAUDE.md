# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Claude Code **skill** (`news-hotspots`) that aggregates multi-source tech/AI news (RSS, Twitter, Google News, Zhihu, Weibo, Toutiao, plus API sources like WallStreetCN/Hacker News) into quality-scored, deduplicated daily/weekly hotspot reports. The skill is invoked by the assistant at runtime; `SKILL.md` is the runtime entry contract and `references/execution-guide.md` is the delivery procedure — both are authoritative for agent behavior.

Path placeholders (`<SKILL_DIR>`, `<WORKSPACE>`, `<DATE>`, `<MODE>`) are the canonical way to express paths in this project. Do **not** rely on the shell's relative cwd — scripts are invoked with absolute `--defaults` / `--config` / `--archive` args.

## Commands

All scripts run via `uv` (Python 3.8+). The unified maintainer entrypoint is `scripts/test-news-hotspots.sh`:

```bash
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh full            # full pipeline → /tmp/news-hotspots/debug + archive
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh step rss|merge|hotspots|validate [--hours N] [--verbose] [--force]
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh unit [tests.test_hotspots tests.test_merge ...]
uv run <SKILL_DIR>/scripts/test-news-hotspots.sh health
```

- **Full pipeline run** (the production flow — sync, ~15–30 min, timeout ≥ 1800s):
  ```bash
  uv run <SKILL_DIR>/scripts/run-pipeline.py \
    --defaults <SKILL_DIR>/config/defaults --config <WORKSPACE>/config \
    --archive <WORKSPACE>/archive/news-hotspots --mode daily --hours 48 --verbose --force
  ```
  daily uses `--hours 48`, weekly uses `--hours 168`.
- **Unit tests**: `uv run python -m unittest discover -s tests` (or a single module: `uv run python -m unittest tests.test_merge`).
- **Config validation** before runs: `uv run <SKILL_DIR>/scripts/validate-config.py --defaults <SKILL_DIR>/config/defaults --config <WORKSPACE>/config --verbose`.
- **Health diagnostics**: `source-health.py --input <archive>/<DATE>/meta` (current run) or `--input <archive>` (auto-aggregates last 7 days).

Do **not** run multiple hotspot tasks concurrently on the same machine — fixed debug/archive paths overwrite each other.

## Architecture

Single-orchestrator pipeline, three-stage data flow:

```
run-pipeline.py  ──parallel fetch (ThreadPoolExecutor)──▶  merge-sources.py  ──▶  merge-hotspots.py
   per-step <step>.json + <step>.meta.json                   merge-sources.json        daily*.json + daily*.md (archived)
```

1. **`run-pipeline.py`** — orchestration + diagnostics + archiving layer. Reads runtime config, fans out all fetch steps in parallel, then runs merge + hotspots sequentially. Writes `pipeline.meta.json` and copies each step's `*.meta.json` into `<DATE>/meta/`. Cleans archives older than `archive_retention_days`. The step list is **not** hardcoded here — it comes from `step_registry.py`.
2. **`step_registry.py`** (`ALL_SOURCE_STEPS`) — the single source of truth for fetch steps: `step_key`, `source_type`, `script_name`, `merge_arg`. Pipeline orchestration, merge input loading, and source_type ordering all derive from this. Adding/removing a source means editing this tuple (and the corresponding `fetch-*.py` + `config/defaults/<source>.json`).
3. **`merge-sources.py`** — normalizes each fetch result into a unified article list, computes `final_score` (base priority + fetch-local rank + history penalty + capped cross-source hotness + recency), runs similarity clustering/dedup (rapidfuzz), groups by `source_type`. Outputs minimal `merge-sources.json`. Tolerates partial fetch results — doesn't require all steps to succeed.
4. **`merge-hotspots.py`** — the **final product generator**, not just a JSON renderer. Re-buckets by `primary_topic`, applies same-day dedup (reads existing `daily*.json` in the day's archive so a re-run yields fresh items), does source-type round-robin selection per topic, diversity-reranks, then writes archived JSON + Markdown. Emits the stable contract markers `ARCHIVED_JSON=` / `ARCHIVED_MARKDOWN=` / `ARCHIVED_MERGED_JSON=` on stdout — `run-pipeline.py` parses these to record output paths.
5. **`source-health.py`** — reads only `meta/*.meta.json` (never re-reads fetch result JSON). `--input` pointing at a date's `meta/` → current-run report; at the archive root → auto-aggregates the last `diagnostics.history_days` (7) days.
6. **`step_contract.py`** — shared helpers for step/meta JSON shape, timing normalization (`timing_s.active` / `timing_s.total`), call stats, slow-request tracking. Fetch scripts import this to keep `<step>.meta.json` shape consistent.

### Configuration system (`config_loader.py`)

Two-layer merge: `config/defaults/*.json` (baseline, in-repo) + `<WORKSPACE>/config/news-hotspots-*.json` (user overlay, gitignored).

- **Record-list configs** (`rss`/`twitter`/`api`/`topics`): merged by `id`. Matching `id` deep-merges, new `id` appends, `"enabled": false` disables a built-in source.
- **`runtime.json`**: deep-merged as a plain dict (not id-keyed). Precedence: CLI flags > workspace runtime overlay > defaults. Holds fetch timeouts/cooldowns/retries/concurrency, pipeline timeouts, archive retention, default top N, and diagnostics thresholds.
- Overlay filenames are fixed (`news-hotspots-rss.json`, `news-hotspots-runtime.json`, etc.); the `news-hotspots-` prefix is how the loader finds them.

### Archive layout (under `<WORKSPACE>/archive/news-hotspots/`)

```
<DATE>/
├── json/       # merge-sources.json, daily.json, daily1.json ... (numbered suffix on re-run → fresh items via same-day dedup)
├── markdown/   # daily.md, daily1.md ...
└── meta/       # pipeline.meta.json, <step>.meta.json (numbered suffix for same-day re-runs)
```

`workspace/` is gitignored — it's the runtime workspace, not part of the skill repo.

## Project conventions (from `.trae/rules/project_rule.md`)

- **No legacy compat layers.** Contract changes must update producer, consumer, tests, and docs in the same commit. Don't reintroduce deprecated fields/flags. Canonical step-diagnostic fields are `items`, `call_stats`, `failed_items` — not `count`. Timing is `timing_s.active`/`timing_s.total`; logs go in `logs`.
- **Deprecated CLI flags** (do not use): `run-pipeline.py` `--archive-dir`/`--debug-dir`/`--output`; `source-health.py` `--input-dir`. Current: `--archive`, `--debug`, `--input`.
- **Single `topic` per source/item.** Sources configure a single-value `topic`; each article carries one `primary_topic`. Subtopics (`ai-frontier`, `ai-infra`, `value-investing`) win over broad categories (`technology`, `business`, `world`, `science`, `social`) when classification is precise. Twitter follows-account sources default to the `twitter` topic (an aggregation bucket); a follows-account may instead set an explicit `topic` (e.g. 段永平 → `value-investing`) to route its tweets into a concrete topic, while `twitter_queries` are always attributed to the concrete topic they belong to.
- **No runtime env vars.** Do not reintroduce `GITHUB_TOKEN` / `GH_APP_ID` / `GH_APP_INSTALL_ID` / `GH_APP_KEY_FILE`. Timeouts and cooldowns live in `runtime.json`, not env vars. RSS fetching honors standard `ALL_PROXY` / `HTTPS_PROXY` / `HTTP_PROXY` (incl. `socks5h://`) when set.
- **Fetch strategy:** same-domain requests are serial by default; rely on per-source `cooldown_s` to avoid rate limits. New fetch scripts should expose an overridable cooldown env var.
- **When CLI params or final-product paths/names change**, sync in one pass: `SKILL.md`, `references/execution-guide.md`, `references/automation-template.md`, `README.md`/`README_CN.md`, `scripts/test-news-hotspots.sh`.
- `merge-sources.py` is intentionally kept as one large staged file — don't split it into new modules prematurely. Same for `run-pipeline.py`'s `main()` (keep it high-level; meta assembly/payload parsing belongs in helpers).
