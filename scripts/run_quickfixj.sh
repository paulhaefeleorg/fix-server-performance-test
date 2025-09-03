#!/bin/bash

# Run QuickFIX/J consumer
QUEUE_PATH=${1:-"./data/fix.q"}
THREAD_COUNT=${2:-"8"}

echo "Starting QuickFIX/J consumer for $QUEUE_PATH with $THREAD_COUNT threads"
./gradlew runQuickFIXJ --args="$QUEUE_PATH $THREAD_COUNT"
