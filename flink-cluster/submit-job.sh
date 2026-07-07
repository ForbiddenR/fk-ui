#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Submit a Flink job JAR to the cluster
# Usage: ./submit-job.sh path/to/my-job.jar [--arguments ...]
# ──────────────────────────────────────────────────────────────
set -euo pipefail

JAR="${1:?Usage: $0 <jar-path> [--additional-args ...]}"
shift

if [ ! -f "$JAR" ]; then
  echo "❌ JAR not found: $JAR"
  exit 1
fi

JAR_ABS="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")"

echo "📦 Submitting: $JAR_ABS"
docker run --rm \
  --network flink-cluster_flink-net \
  -v "$JAR_ABS:/job.jar:ro" \
  flink:2.3.0-java17 \
  flink run \
    -m jobmanager:8081 \
    /job.jar "$@"

echo "✅ Job submitted."
echo "   Web UI → http://localhost:8081"
