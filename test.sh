#!/bin/sh
cargo build && time target/debug/sqllogs_agg.exe 
