#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="/home/alan/gits/dragonreef.net"
helper_path="${script_dir}/scripts/refresh_chart_cache.mjs"
default_output_dir="${HOME}/investing/chart-cache"
remote_target_default="root@dragonreef.net:/opt/tomcat/webapps/ROOT/WEB-INF/chart-cache/"
archive_dir_default="${HOME}/investing/chart-archive"

if [[ ! -f "$helper_path" ]]; then
  helper_path="${repo_dir}/scripts/refresh_chart_cache.mjs"
fi

if [[ $# -lt 1 ]]; then
  echo "usage: refresh_chart_cache.sh [--output-dir DIR] [--push] [--remote-target TARGET] [--archive-monthly] [--archive-dir DIR] TICKER [TICKER ...]" >&2
  exit 2
fi

output_dir="$default_output_dir"
push_after_refresh=0
remote_target="$remote_target_default"
archive_monthly=0
archive_dir="$archive_dir_default"
tickers=()
forward_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      if [[ $# -lt 2 ]]; then
        echo "--output-dir requires a value" >&2
        exit 2
      fi
      output_dir="$2"
      forward_args+=("$1" "$2")
      shift 2
      ;;
    --push)
      push_after_refresh=1
      shift
      ;;
    --remote-target)
      if [[ $# -lt 2 ]]; then
        echo "--remote-target requires a value" >&2
        exit 2
      fi
      remote_target="$2"
      shift 2
      ;;
    --archive-monthly)
      archive_monthly=1
      shift
      ;;
    --archive-dir)
      if [[ $# -lt 2 ]]; then
        echo "--archive-dir requires a value" >&2
        exit 2
      fi
      archive_dir="$2"
      shift 2
      ;;
    --*)
      forward_args+=("$1")
      shift
      ;;
    *)
      tickers+=("$1")
      forward_args+=("$1")
      shift
      ;;
  esac
done

if [[ ${#tickers[@]} -eq 0 ]]; then
  echo "at least one ticker is required" >&2
  exit 2
fi

if [[ ! -d "${repo_dir}/node_modules/playwright-core" ]]; then
  echo "playwright-core not found in ${repo_dir}/node_modules" >&2
  echo "run: cd ${repo_dir} && npm install" >&2
  exit 1
fi

node "$helper_path" "${forward_args[@]}"

if [[ "$archive_monthly" -eq 1 ]]; then
  archive_stamp="$(date +%Y-%m)"
  for ticker in "${tickers[@]}"; do
    src_file="${output_dir}/${ticker}.csv"
    ticker_archive_dir="${archive_dir}/${ticker}"
    archive_file="${ticker_archive_dir}/${archive_stamp}.csv"
    mkdir -p "$ticker_archive_dir"
    if [[ ! -f "$archive_file" ]]; then
      cp "$src_file" "$archive_file"
      echo "archived ${ticker} -> ${archive_file}"
    fi
  done
fi

if [[ "$push_after_refresh" -eq 1 ]]; then
  files=()
  for ticker in "${tickers[@]}"; do
    files+=("${output_dir}/${ticker}.csv")
  done
  scp "${files[@]}" "$remote_target"
fi
