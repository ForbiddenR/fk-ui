#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Open an interactive Flink SQL client connected to the cluster
# ──────────────────────────────────────────────────────────────
set -euo pipefail

docker run --rm -it \
  --network flink-cluster_flink-net \
  flink:2.3.0-java17 \
  sql-client.sh
