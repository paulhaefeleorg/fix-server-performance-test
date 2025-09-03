#!/bin/bash

# Run FIX message generator
QUEUE_PATH=${1:-"./data/fix.q"}
MESSAGE_COUNT=${2:-"2000000"}

echo "Generating $MESSAGE_COUNT FIX messages to $QUEUE_PATH"
./gradlew runGenerate --args="$QUEUE_PATH $MESSAGE_COUNT"
