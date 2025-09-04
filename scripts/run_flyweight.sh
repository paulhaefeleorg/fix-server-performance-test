#!/bin/bash

# Run flyweight consumer via Application plugin so JVM flags apply
QUEUE_PATH=${1:-"./data/fix.q"}
METRICS_PATH=${2:-"./metrics/fly.txt"}

echo "Starting flyweight consumer for $QUEUE_PATH (metrics path: $METRICS_PATH)"
./gradlew -q run --args="flyweight $QUEUE_PATH $METRICS_PATH"
