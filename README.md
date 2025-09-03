# fix-server-performance-test

## What this does
Compare two ingestion/processing paths for FIX.4.4 using Chronicle Queue:
1) Flyweight/off-heap on a single pinned thread
2) QuickFIX/J parse + thread pool

Accept/Done

Generates valid FIX with SOH=0x01, BodyLength(9), CheckSum(10), and tag 50001=tsNanos

Flyweight shows lower p99 and fewer GCs than QFJ path

CSV/console metrics written to ./metrics/*

## How to run
```bash
scripts/clean_queue.sh
scripts/run_generate.sh ./data/fix.q 2000000
scripts/run_flyweight.sh ./data/fix.q true
scripts/run_quickfixj.sh ./data/fix.q 8
