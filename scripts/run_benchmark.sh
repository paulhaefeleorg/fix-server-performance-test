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
QFJ_GC="$METRICS_DIR/gc-qfj.txt"
FLY_GC="$METRICS_DIR/gc-fly.txt"

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
if [ -f "$QFJ_GC" ]; then
  echo "  GC:"
  sed -n '1,999p' "$QFJ_GC" | sed 's/^/    /'
  # Peak memory (JMX)
  QFJ_PEAK_BYTES=$(sed -n '/^mem_peaks_jmx=/,/^mem_peaks_tracked=/p' "$QFJ_GC" | grep 'usedBytes=' | sed -E 's/.*usedBytes=([0-9]+).*/\1/' | sort -n | tail -n1)
  if [ -n "${QFJ_PEAK_BYTES:-}" ]; then
    QFJ_PEAK_MB=$(awk "BEGIN {printf \"%.1f\", $QFJ_PEAK_BYTES/1024/1024}")
    echo "    PeakUsed(JMX): ${QFJ_PEAK_MB} MB"
  fi
  # Peak memory (Tracked)
  QFJ_TR_PEAK_BYTES=$(sed -n '/^mem_peaks_tracked=/,$p' "$QFJ_GC" | grep 'usedBytes=' | sed -E 's/.*usedBytes=([0-9]+).*/\1/' | sort -n | tail -n1)
  if [ -n "${QFJ_TR_PEAK_BYTES:-}" ]; then
    QFJ_TR_PEAK_MB=$(awk "BEGIN {printf \"%.1f\", $QFJ_TR_PEAK_BYTES/1024/1024}")
    echo "    PeakUsed(Tracked): ${QFJ_TR_PEAK_MB} MB"
  fi
fi
echo
printf "%-12s %s\n" "Flyweight" "$FLY_METRICS"
sed -n '1,999p' "$FLY_METRICS" | sed 's/^/  /'
if [ -f "$FLY_GC" ]; then
  echo "  GC:"
  sed -n '1,999p' "$FLY_GC" | sed 's/^/    /'
  FLY_PEAK_BYTES=$(sed -n '/^mem_peaks_jmx=/,/^mem_peaks_tracked=/p' "$FLY_GC" | grep 'usedBytes=' | sed -E 's/.*usedBytes=([0-9]+).*/\1/' | sort -n | tail -n1)
  if [ -n "${FLY_PEAK_BYTES:-}" ]; then
    FLY_PEAK_MB=$(awk "BEGIN {printf \"%.1f\", $FLY_PEAK_BYTES/1024/1024}")
    echo "    PeakUsed(JMX): ${FLY_PEAK_MB} MB"
  fi
  FLY_TR_PEAK_BYTES=$(sed -n '/^mem_peaks_tracked=/,$p' "$FLY_GC" | grep 'usedBytes=' | sed -E 's/.*usedBytes=([0-9]+).*/\1/' | sort -n | tail -n1)
  if [ -n "${FLY_TR_PEAK_BYTES:-}" ]; then
    FLY_TR_PEAK_MB=$(awk "BEGIN {printf \"%.1f\", $FLY_TR_PEAK_BYTES/1024/1024}")
    echo "    PeakUsed(Tracked): ${FLY_TR_PEAK_MB} MB"
  fi
fi
echo "=================================================="

echo "Done."


