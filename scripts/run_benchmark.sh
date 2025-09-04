#!/usr/bin/env bash
set -euo pipefail

# Usage: scripts/run_benchmark.sh <num_messages> <threads>
# Defaults: num_messages=200000, threads=8

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
cd "$ROOT_DIR"

NUM_MESSAGES=${1:-200000}
THREADS=${2:-8}

DATA_DIR="./data"
METRICS_DIR="./metrics"
Q_PATH="$DATA_DIR/fix.q"
QFJ_METRICS="$METRICS_DIR/qfj.txt"
FLY_METRICS="$METRICS_DIR/fly.txt"

mkdir -p "$DATA_DIR" "$METRICS_DIR"

# Helper to reset the queue between runs
reset_queue() {
  if [ -e "$Q_PATH" ]; then
    rm -rf "$Q_PATH"
  fi
}

echo "[1/5] Reset queue directory"
reset_queue

echo "[2/5] Generate $NUM_MESSAGES messages"
./gradlew -q run --args="generate $Q_PATH $NUM_MESSAGES"

echo "[3/5] QuickFIX/J consume (threads=$THREADS), metrics -> $QFJ_METRICS"
./gradlew -q run --args="quickfixj $Q_PATH $THREADS $QFJ_METRICS"

echo "[4/5] Reset queue directory and regenerate $NUM_MESSAGES messages"
reset_queue
./gradlew -q run --args="generate $Q_PATH $NUM_MESSAGES"

echo "[5/5] Flyweight consume, metrics -> $FLY_METRICS"
./gradlew -q run --args="flyweight $Q_PATH $FLY_METRICS"

echo
echo "================ Benchmark Results ================"
printf "%-12s %s\n" "Consumer" "Metrics"
printf "%-12s %s\n" "--------" "-------"
printf "%-12s %s\n" "QuickFIXJ" "$QFJ_METRICS"
sed -n '1,999p' "$QFJ_METRICS" | sed 's/^/  /'
echo
printf "%-12s %s\n" "Flyweight" "$FLY_METRICS"
sed -n '1,999p' "$FLY_METRICS" | sed 's/^/  /'
echo "=================================================="

echo "Done."


