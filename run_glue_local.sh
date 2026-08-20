#!/usr/bin/env bash
# Runs the reference Glue job locally with real Spark against MinIO (S3) and
# Redis, proving the exact pattern that runs on AWS Glue: parquet read with
# partition discovery, window dedupe, parallel pipelined writes, index
# creation, validation. Requires ./run_demo.sh to have seeded MinIO first.

set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
NET="s3fh_default"
SPARK_IMG="apache/spark:3.5.1"
cd "$HERE"

CONTRACT='{
  "feature_group": "customer-features",
  "entity_column": "customer_id",
  "event_time_column": "event_time",
  "write_time_column": "write_time",
  "deleted_column": "is_deleted",
  "key_prefix": "fs-glue:customer-features",
  "ttl_seconds": 0,
  "features": {
    "fraud_score": "NUMERIC",
    "credit_score": "NUMERIC",
    "avg_ticket_30d": "NUMERIC",
    "tx_count_24h": "NUMERIC",
    "device_trust": "NUMERIC",
    "segment": "TAG",
    "is_pep": "TAG"
  }
}'

docker run --rm --network "$NET" -u root \
  -v "$HERE":/job -w /job \
  -v s3fh_ivy:/tmp/ivy \
  -e HOME=/tmp \
  --entrypoint /bin/bash "$SPARK_IMG" -c "
python3 -m pip install --quiet --user redis==5.2.0 2>/dev/null || pip3 install --quiet --user redis==5.2.0
/opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  glue_feature_materializer.py \
    --source-path 's3a://sagemaker-offline-store-demo/feature-store/111122223333/sagemaker/us-east-1/offline-store/customer-features-1755600000/data/' \
    --contract '$CONTRACT' \
    --redis-host redis --redis-port 6379 \
    --create-index --sample 25
" 2>&1 | grep -vE "^\s*(INFO|WARN) (Task|Executor|BlockManager|MemoryStore|ShuffleBlock|DAGScheduler|TaskSetManager|TaskSchedulerImpl|SparkContext|Utils|SecurityManager)"
CODE=${PIPESTATUS[0]}

echo ""
echo "glue-local exit code: $CODE"
echo "Proof from Redis:"
docker compose exec -T redis redis-cli HGETALL fs-glue:customer-features:C000042
docker compose exec -T redis redis-cli FT.INFO idx:fs-glue:customer-features | head -6
exit $CODE
