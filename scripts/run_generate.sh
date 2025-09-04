#!/bin/bash

# Run FIX message generator via Application plugin so JVM flags apply
QUEUE_PATH=${1:-"./data/fix.q"}
MESSAGE_COUNT=${2:-"2000000"}

echo "Generating $MESSAGE_COUNT FIX messages to $QUEUE_PATH"
./gradlew -q run --args="generate $QUEUE_PATH $MESSAGE_COUNT"
