#!/bin/bash
# run-and-plot.sh

TIMESTAMP=$(date +"%Y%m%dT%H%M%S")

echo "Starting pipeline with timestamp: $TIMESTAMP"

# 1. Run the experiments
./run-experiments.sh "$TIMESTAMP"

# 2. Plot the results
./plot-experiments.sh "$TIMESTAMP"

echo "Pipeline complete! Check the experiments/${TIMESTAMP} folder."
