#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP_DIR="/tmp/news-hotspots"
DEBUG_DIR="$TMP_DIR/debug"

DEFAULTS_DIR="$ROOT_DIR/config/defaults"
DEFAULT_CONFIG_DIR="$ROOT_DIR/workspace/config"
DEFAULT_ARCHIVE_ROOT_DIR="$ROOT_DIR/workspace/archive/news-hotspots"

MERGED_JSON="$DEBUG_DIR/merged.json"
RSS_JSON="$DEBUG_DIR/rss.json"
GITHUB_JSON="$DEBUG_DIR/github.json"
TRENDING_JSON="$DEBUG_DIR/trending.json"
API_JSON="$DEBUG_DIR/api.json"
TWITTER_JSON="$DEBUG_DIR/twitter.json"
REDDIT_JSON="$DEBUG_DIR/reddit.json"
V2EX_JSON="$DEBUG_DIR/v2ex.json"
GOOGLE_JSON="$DEBUG_DIR/google.json"

HOURS=48
CONFIG_DIR=""
VERBOSE=false
FORCE=false
SKIP_STEPS=""
UNIT_MODULES=""
STEP_OUTPUT_DIR="$DEBUG_DIR"

fetch_step_script() {
  case "$1" in
    rss) echo "$SCRIPT_DIR/fetch-rss.py" ;;
    github) echo "$SCRIPT_DIR/fetch-github.py" ;;
    trending) echo "$SCRIPT_DIR/fetch-github-trending.py" ;;
    api) echo "$SCRIPT_DIR/fetch-api.py" ;;
    twitter) echo "$SCRIPT_DIR/fetch-twitter.py" ;;
    reddit) echo "$SCRIPT_DIR/fetch-reddit.py" ;;
    v2ex) echo "$SCRIPT_DIR/fetch-v2ex.py" ;;
    google) echo "$SCRIPT_DIR/fetch-google.py" ;;
    *) return 1 ;;
  esac
}

step_output_path() {
  case "$1" in
    rss) echo "$RSS_JSON" ;;
    github) echo "$GITHUB_JSON" ;;
    trending) echo "$TRENDING_JSON" ;;
    api) echo "$API_JSON" ;;
    twitter) echo "$TWITTER_JSON" ;;
    reddit) echo "$REDDIT_JSON" ;;
    v2ex) echo "$V2EX_JSON" ;;
    google) echo "$GOOGLE_JSON" ;;
    merge) echo "$MERGED_JSON" ;;
    hotspots) echo "$STEP_OUTPUT_DIR/merge-hotspots.json" ;;
    *) return 1 ;;
  esac
}

usage() {
  cat <<'HELP'
Unified maintainer test entrypoint for news-hotspots.

USAGE:
  ./scripts/test-news-hotspots.sh full [--hours N] [--config DIR] [--verbose] [--force] [--skip a,b]
  ./scripts/test-news-hotspots.sh step <rss|github|trending|api|twitter|reddit|v2ex|google|merge|hotspots|validate> [--hours N] [--config DIR] [--verbose] [--force]
  ./scripts/test-news-hotspots.sh health [--verbose]
  ./scripts/test-news-hotspots.sh unit [tests.test_hotspots tests.test_merge ...]

OUTPUTS:
  full:
    /tmp/news-hotspots/debug/pipeline.meta.json
    /tmp/news-hotspots/debug/rss.meta.json
    /tmp/news-hotspots/debug/twitter.meta.json
    /tmp/news-hotspots/debug/google.meta.json
    /tmp/news-hotspots/debug/github.meta.json
    /tmp/news-hotspots/debug/trending.meta.json
    /tmp/news-hotspots/debug/api.meta.json
    /tmp/news-hotspots/debug/v2ex.meta.json
    /tmp/news-hotspots/debug/reddit.meta.json
    /tmp/news-hotspots/debug/merge.meta.json
    /tmp/news-hotspots/debug/merge-hotspots.meta.json
    /tmp/news-hotspots/debug/rss.json
    /tmp/news-hotspots/debug/twitter.json
    /tmp/news-hotspots/debug/google.json
    /tmp/news-hotspots/debug/github.json
    /tmp/news-hotspots/debug/trending.json
    /tmp/news-hotspots/debug/api.json
    /tmp/news-hotspots/debug/v2ex.json
    /tmp/news-hotspots/debug/reddit.json
    /tmp/news-hotspots/debug/merged.json
    /tmp/news-hotspots/debug/merge-hotspots.json
    workspace/archive/news-hotspots/<DATE>/json/hotspots.json
    workspace/archive/news-hotspots/<DATE>/markdown/hotspots.md
    workspace/archive/news-hotspots/<DATE>/meta/*.meta.json
  step:
    /tmp/news-hotspots/debug/rss.json
    /tmp/news-hotspots/debug/github.json
    /tmp/news-hotspots/debug/trending.json
    /tmp/news-hotspots/debug/api.json
    /tmp/news-hotspots/debug/twitter.json
    /tmp/news-hotspots/debug/reddit.json
    /tmp/news-hotspots/debug/v2ex.json
    /tmp/news-hotspots/debug/google.json
    /tmp/news-hotspots/debug/merged.json
    /tmp/news-hotspots/debug/merge-hotspots.json
    workspace/archive/news-hotspots/<DATE>/json/hotspots.json
    workspace/archive/news-hotspots/<DATE>/markdown/hotspots.md
  health:
    直接输出诊断报告到控制台
HELP
}

clean_tmp_dir() {
  rm -rf "$TMP_DIR"
  mkdir -p "$TMP_DIR" "$DEBUG_DIR"
}

bool_flag() {
  local flag_name="$1"
  local enabled="$2"
  if [ "$enabled" = true ]; then
    printf '%s\n' "$flag_name"
  fi
}

run_cmd() {
  echo "+ $*"
  "$@"
}

append_common_fetch_args() {
  local -n cmd_ref=$1
  local include_defaults="${2:-true}"
  if [ "$include_defaults" = true ]; then
    cmd_ref+=(--defaults "$DEFAULTS_DIR")
  fi
  if [ -n "$CONFIG_DIR" ] && [ -d "$CONFIG_DIR" ]; then
    cmd_ref+=(--config "$CONFIG_DIR")
  fi
  cmd_ref+=(--hours "$HOURS")
  if [ "$VERBOSE" = true ]; then
    cmd_ref+=(--verbose)
  fi
  if [ "$FORCE" = true ]; then
    cmd_ref+=(--force)
  fi
}

parse_common_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --hours) HOURS="$2"; shift 2 ;;
      --config) CONFIG_DIR="$2"; shift 2 ;;
      --verbose|-v) VERBOSE=true; shift ;;
      --force) FORCE=true; shift ;;
      --skip) SKIP_STEPS="$2"; shift 2 ;;
      --help|-h) usage; exit 0 ;;
      *)
        if [ -z "$UNIT_MODULES" ]; then
          UNIT_MODULES="$1"
        else
          UNIT_MODULES="$UNIT_MODULES $1"
        fi
        shift
        ;;
    esac
  done
}

run_full() {
  clean_tmp_dir
  local cmd=(
    uv run "$SCRIPT_DIR/run-pipeline.py"
    --defaults "$DEFAULTS_DIR"
    --hours "$HOURS"
    --archive "$DEFAULT_ARCHIVE_ROOT_DIR"
    --debug "$DEBUG_DIR"
  )
  if [ -n "$CONFIG_DIR" ] && [ -d "$CONFIG_DIR" ]; then
    cmd+=(--config "$CONFIG_DIR")
  fi
  if [ "$VERBOSE" = true ]; then
    cmd+=(--verbose)
  fi
  if [ "$FORCE" = true ]; then
    cmd+=(--force)
  fi
  if [ -n "$SKIP_STEPS" ]; then
    cmd+=(--skip "$SKIP_STEPS")
  fi
  run_cmd "${cmd[@]}"
}

run_fetch_step() {
  local step="$1"
  local output_path
  output_path="$(step_output_path "$step")"
  local script_path
  script_path="$(fetch_step_script "$step")"
  mkdir -p "$TMP_DIR" "$DEBUG_DIR"
  local cmd=(uv run "$script_path")
  case "$step" in
    api|v2ex)
      if [ "$VERBOSE" = true ]; then
        cmd+=(--verbose)
      fi
      if [ "$FORCE" = true ]; then
        cmd+=(--force)
      fi
      ;;
    *)
      append_common_fetch_args cmd true
      ;;
  esac
  cmd+=(--output "$output_path")
  run_cmd "${cmd[@]}"
  [ -f "$output_path" ] || { echo "Missing output: $output_path" >&2; exit 1; }
}

run_step() {
  local step="$1"
  shift
  mkdir -p "$TMP_DIR" "$DEBUG_DIR"

  case "$step" in
    validate)
      local cmd=(uv run "$SCRIPT_DIR/validate-config.py" --defaults "$DEFAULTS_DIR")
      if [ -n "$CONFIG_DIR" ] && [ -d "$CONFIG_DIR" ]; then
        cmd+=(--config "$CONFIG_DIR")
      fi
      if [ "$VERBOSE" = true ]; then
        cmd+=(--verbose)
      fi
      run_cmd "${cmd[@]}"
      ;;
    rss|github|trending|api|twitter|reddit|v2ex|google)
      run_fetch_step "$step"
      ;;
    merge)
      local cmd=(uv run "$SCRIPT_DIR/merge-sources.py" --output "$MERGED_JSON" --archive "$DEFAULT_ARCHIVE_ROOT_DIR")
      for pair in \
        "--rss:$RSS_JSON" \
        "--github:$GITHUB_JSON" \
        "--trending:$TRENDING_JSON" \
        "--api:$API_JSON" \
        "--twitter:$TWITTER_JSON" \
        "--reddit:$REDDIT_JSON" \
        "--v2ex:$V2EX_JSON" \
        "--google:$GOOGLE_JSON"; do
        local flag="${pair%%:*}"
        local path="${pair#*:}"
        if [ -f "$path" ]; then
          cmd+=("$flag" "$path")
        fi
      done
      if [ "$VERBOSE" = true ]; then
        cmd+=(--verbose)
      fi
      run_cmd "${cmd[@]}"
      [ -f "$MERGED_JSON" ] || { echo "Missing output: $MERGED_JSON" >&2; exit 1; }
      ;;
    hotspots)
      [ -f "$MERGED_JSON" ] || { echo "Missing input: $MERGED_JSON" >&2; exit 1; }
      local cmd=(uv run "$SCRIPT_DIR/merge-hotspots.py" --input "$MERGED_JSON" --archive "$DEFAULT_ARCHIVE_ROOT_DIR" --debug "$DEBUG_DIR" --top 15)
      run_cmd "${cmd[@]}"
      [ -f "$DEBUG_DIR/merge-hotspots.json" ] || { echo "Missing output: $DEBUG_DIR/merge-hotspots.json" >&2; exit 1; }
      ;;
    *)
      echo "Unknown step: $step" >&2
      exit 1
      ;;
  esac
}

run_unit() {
  local modules=(
    tests.test_source_health
    tests.test_run_pipeline
    tests.test_hotspots
    tests.test_merge
    tests.test_config
  )
  if [ -n "$UNIT_MODULES" ]; then
    # shellcheck disable=SC2206
    modules=($UNIT_MODULES)
  fi
  run_cmd uv run python -m unittest "${modules[@]}"
}

run_health() {
  mkdir -p "$STEP_OUTPUT_DIR"
  local cmd=(uv run "$SCRIPT_DIR/source-health.py" --input "$DEBUG_DIR")
  if [ "$VERBOSE" = true ]; then
    cmd+=(--verbose)
  fi
  run_cmd "${cmd[@]}"
}

main() {
  if [ $# -lt 1 ]; then
    usage
    exit 1
  fi

  local mode="$1"
  shift

  case "$mode" in
    full)
      parse_common_args "$@"
      run_full
      ;;
    step)
      if [ $# -lt 1 ]; then
        usage
        exit 1
      fi
      local step_name="$1"
      shift
      parse_common_args "$@"
      run_step "$step_name"
      ;;
    unit)
      parse_common_args "$@"
      run_unit
      ;;
    health)
      parse_common_args "$@"
      run_health
      ;;
    --help|-h|help)
      usage
      ;;
    *)
      echo "Unknown mode: $mode" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
