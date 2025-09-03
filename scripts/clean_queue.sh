#!/bin/bash

# Clean Chronicle Queue data
echo "Cleaning Chronicle Queue data..."
rm -rf ./data/fix.q
mkdir -p ./data
echo "Queue cleaned successfully"
