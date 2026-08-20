#!/usr/bin/env bash
# Load the deduped snapshot with RIOT-X reading Parquet straight from S3.
# In production this is: Athena UNLOAD (dedupe) -> riotx file-import s3://...
# Locally, MinIO plays S3: pass --s3-endpoint (RIOT-X ignores AWS_ENDPOINT_URL
# env vars) and pin virtual-host DNS with --add-host + MINIO_DOMAIN=minio in
# compose. On real AWS S3 none of that is needed.

set -u
NET="s3fh_default"
BUCKET="sagemaker-offline-store-demo"
SNAPSHOT="s3://${BUCKET}/snapshot/customer-features/snapshot.parquet"

MINIO_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' s3fh-minio-1 2>/dev/null)
if [ -z "$MINIO_IP" ]; then
  echo "MinIO container not found. Run ./run_demo.sh first."
  exit 1
fi

echo "=== RIOT-X file-import from S3 (Parquet) into Redis db 1 ==="
docker run --rm --network "$NET" \
  --add-host "${BUCKET}.minio:${MINIO_IP}" \
  riotx/riotx file-import "$SNAPSHOT" \
    --s3-endpoint http://minio:9000 \
    --s3-region us-east-1 \
    --s3-access minioadmin \
    --s3-secret minioadmin \
    -u "redis://redis:6379/1" \
    hset "fs-riotx:customer-features:#{customer_id}"
CODE=$?

if [ $CODE -eq 0 ]; then
  echo ""
  echo "RIOT-X load OK. Sample from db 1:"
  docker compose exec -T redis redis-cli -n 1 HGETALL fs-riotx:customer-features:C000042
  docker compose exec -T redis redis-cli -n 1 DBSIZE
else
  echo ""
  echo "RIOT-X local attempt failed (exit=$CODE). Likely MinIO endpoint/addressing quirks."
  echo "On real AWS S3 the documented syntax is:"
  echo "  riotx file-import s3://bucket/snapshot/*.parquet --s3-region us-east-1 \\"
  echo "    -u rediss://default:PASSWORD@host:port hset \"fs:customer-features:#{customer_id}\""
fi
exit $CODE
