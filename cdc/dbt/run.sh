#!/bin/sh
# Wrapper so `dbt run` for this project can be invoked from anywhere
# (the repo root, cdc/, wherever) without hitting the CWD-relative-path
# footgun: dbt-duckdb's `path: cdc_raw.duckdb` in profiles.yml resolves
# relative to the directory `dbt` is actually invoked from, not
# --project-dir -- so `dbt run --project-dir cdc/dbt --profiles-dir
# cdc/dbt` run from elsewhere silently creates the DuckDB file in the
# wrong place instead of erroring. cd'ing here first, inside the script,
# sidesteps that regardless of where the caller's own shell is sitting.
#
# Usage (from anywhere):
#   ./cdc/dbt/run.sh              # dbt run
#   ./cdc/dbt/run.sh build        # any dbt subcommand + args pass through
set -eu
cd "$(dirname "$0")"
exec ../../.venv/bin/dbt "${1:-run}" --profiles-dir . "${@:2}"
