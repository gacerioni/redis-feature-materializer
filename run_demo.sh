#!/usr/bin/env bash
# End-to-end demo: SageMaker Feature Store offline store (S3/Parquet) -> Redis.
# Fully local: MinIO plays S3, no AWS account needed.

set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
NET="s3fh_default"
IMG="s3fh/hydrator:latest"
cd "$HERE"

echo "=== [1/5] stack up (MinIO + Redis 8) ==="
docker compose up -d || exit 1

echo "=== [2/5] build hydrator image ==="
docker build -q -t "$IMG" . || exit 1

echo "=== [3/5] generate the offline store (Glue-format layout, tombstones, multi-version) ==="
docker run --rm --network "$NET" \
  -e S3_ENDPOINT_URL=http://minio:9000 \
  "$IMG" python make_offline_store.py || exit 1

echo ""
echo "=== [4/5] hydrate Redis (dedupe latest per entity, drop tombstones) ==="
docker run --rm --network "$NET" \
  -e S3_ENDPOINT_URL=http://minio:9000 -e REDIS_HOST=redis -e REDIS_PORT=6379 \
  "$IMG" python hydrate.py --sample 50 --export-snapshot snapshot/customer-features
HYDRATE=$?

echo ""
echo "=== [5/5] proof: one entity straight from Redis ==="
docker compose exec -T redis redis-cli HGETALL fs:customer-features:C000042
docker compose exec -T redis redis-cli DBSIZE

echo ""
echo "hydrate exit code: $HYDRATE (0 = clean, validated)"
echo "MinIO console: http://localhost:9003 (minioadmin/minioadmin)"
echo "Redis: localhost:6398"
