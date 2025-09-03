#!/bin/bash

# Run flyweight consumer
QUEUE_PATH=${1:-"./data/fix.q"}
ENABLE_METRICS=${2:-"true"}

echo "Starting flyweight consumer for $QUEUE_PATH (metrics: $ENABLE_METRICS)"
./gradlew runFlyweight --args="$QUEUE_PATH $ENABLE_METRICS"
