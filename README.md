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
```

### Scripts

- **scripts/clean_queue.sh**: Remove and recreate the Chronicle Queue data directory.
  - Usage:
    ```bash
    scripts/clean_queue.sh
    ```

- **scripts/run_generate.sh**: Generate FIX messages into a queue file.
  - Usage:
    ```bash
    scripts/run_generate.sh <queue_path> <message_count>
    # Example
    scripts/run_generate.sh ./data/fix.q 2000000
    ```

- **scripts/run_flyweight.sh**: Run the flyweight consumer against a queue.
  - Usage:
    ```bash
    scripts/run_flyweight.sh <queue_path> [enable_metrics]
    # Example
    scripts/run_flyweight.sh ./data/fix.q true
    ```

- **scripts/run_quickfixj.sh**: Run the QuickFIX/J consumer with a thread count.
  - Usage:
    ```bash
    scripts/run_quickfixj.sh <queue_path> <thread_count>
    # Example
    scripts/run_quickfixj.sh ./data/fix.q 8
    ```

- **scripts/run_benchmark.sh**: End-to-end benchmark runner that generates messages, runs both consumers, and prints metrics locations and summaries.
  - Usage:
    ```bash
    scripts/run_benchmark.sh <num_messages> <threads>
    # Defaults: num_messages=200000, threads=8
    # Example
    scripts/run_benchmark.sh 2000000 8
    ```
