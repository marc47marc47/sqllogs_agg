#!/bin/sh
file=${1-data/sql_logs.tsv}
cargo build && time target/debug/sqllogs_agg.exe ${file}
