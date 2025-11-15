#!/bin/bash
set -e

# Entrypoint script for ECS/Fargate container
# Supports dataset_id from:
# 1. Command line arguments (passed via CMD in Dockerfile or Task Definition)
# 2. Environment variable DATASET_ID (set in ECS Task Definition)

# If arguments are provided, use them
if [ $# -gt 0 ]; then
    echo "Running ETL with dataset_id from command line: $1"
    python -m src.cli "$@"
else
    # Otherwise, check environment variable
    if [ -z "$DATASET_ID" ]; then
        echo "Error: DATASET_ID environment variable is not set and no arguments provided" >&2
        echo "Usage: docker-entrypoint.sh <dataset_id> OR set DATASET_ID env var" >&2
        exit 1
    fi
    
    echo "Running ETL with dataset_id from environment: $DATASET_ID"
    python -m src.cli "$DATASET_ID"
fi

