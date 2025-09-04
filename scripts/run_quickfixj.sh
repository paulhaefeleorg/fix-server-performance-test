#!/bin/bash

# Run QuickFIX/J consumer via Application plugin so JVM flags apply
QUEUE_PATH=${1:-"./data/fix.q"}
THREAD_COUNT=${2:-"8"}

echo "Starting QuickFIX/J consumer for $QUEUE_PATH with $THREAD_COUNT threads"
./gradlew -q run --args="quickfixj $QUEUE_PATH $THREAD_COUNT"
