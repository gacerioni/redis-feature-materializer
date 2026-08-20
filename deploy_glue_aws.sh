#!/usr/bin/env bash
# Deploys the reference feature materializer as a REAL AWS Glue job.
#
#   ./deploy_glue_aws.sh setup   # bucket + demo offline store + IAM role + Glue job
#   REDIS_HOST=... REDIS_PORT=... REDIS_PASSWORD=... ./deploy_glue_aws.sh run
#   ./deploy_glue_aws.sh teardown  # removes job, role and bucket
#
# Cost per run: 2 workers G.1X for ~3 minutes (about USD 0.05).

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REGION="${AWS_REGION:-us-east-1}"
ACCT=$(aws sts get-caller-identity --query Account --output text) || exit 1
BUCKET="${BUCKET:-gabs-fs-glue-demo-${ACCT}}"
JOB="gabs-fs-materializer-demo"
ROLE="GabsFSMaterializerGlueRole"
DATA_PREFIX="feature-store/111122223333/sagemaker/us-east-1/offline-store/customer-features-1755600000/data"
ACTION="${1:-setup}"
cd "$HERE"

setup() {
  echo "=== [1/5] S3 bucket: s3://$BUCKET ==="
  aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || aws s3 mb "s3://$BUCKET" --region "$REGION" || exit 1

  echo "=== [2/5] seed demo offline store into real S3 ==="
  docker run --rm -v "$HOME/.aws":/root/.aws:ro -v "$HERE":/app -w /app \
    -e S3_ENDPOINT_URL=aws -e BUCKET="$BUCKET" -e AWS_REGION="$REGION" \
    -e AWS_PROFILE="${AWS_PROFILE:-default}" \
    s3fh/hydrator:latest python make_offline_store.py | tail -3 || exit 1

  echo "=== [3/5] upload job script and contract ==="
  cat > "$HERE/contract_aws.json" <<'EOF'
{
  "feature_group": "customer-features",
  "entity_column": "customer_id",
  "event_time_column": "event_time",
  "write_time_column": "write_time",
  "deleted_column": "is_deleted",
  "key_prefix": "fs:customer-features",
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
}
EOF
  aws s3 cp glue_feature_materializer.py "s3://$BUCKET/glue/" || exit 1
  aws s3 cp contract_aws.json "s3://$BUCKET/glue/contract.json" || exit 1

  echo "=== [4/5] IAM role for Glue ==="
  if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    aws iam create-role --role-name "$ROLE" --assume-role-policy-document '{
      "Version": "2012-10-17",
      "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]
    }' >/dev/null || exit 1
    aws iam attach-role-policy --role-name "$ROLE" \
      --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole || exit 1
    aws iam put-role-policy --role-name "$ROLE" --policy-name s3-demo-bucket --policy-document "{
      \"Version\": \"2012-10-17\",
      \"Statement\": [{\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:ListBucket\"],
        \"Resource\": [\"arn:aws:s3:::$BUCKET\", \"arn:aws:s3:::$BUCKET/*\"]}]
    }" || exit 1
    echo "role created: $ROLE (waiting 10s for propagation)"
    sleep 10
  else
    echo "role exists: $ROLE"
  fi

  echo "=== [5/5] Glue job ==="
  if ! aws glue get-job --job-name "$JOB" >/dev/null 2>&1; then
    aws glue create-job --name "$JOB" --role "$ROLE" \
      --command "Name=glueetl,ScriptLocation=s3://$BUCKET/glue/glue_feature_materializer.py,PythonVersion=3" \
      --glue-version "5.0" --number-of-workers 2 --worker-type G.1X \
      --default-arguments '{"--additional-python-modules": "redis==5.2.0"}' >/dev/null || exit 1
    echo "job created: $JOB"
  else
    echo "job exists: $JOB (script re-uploaded)"
  fi
  echo ""
  echo "Setup done. Now: REDIS_HOST=... REDIS_PORT=... REDIS_PASSWORD=... $0 run"
}

run() {
  : "${REDIS_HOST:?set REDIS_HOST}" "${REDIS_PORT:?set REDIS_PORT}" "${REDIS_PASSWORD:?set REDIS_PASSWORD}"
  echo "=== start Glue job run ==="
  RUN_ID=$(aws glue start-job-run --job-name "$JOB" --arguments "{
    \"--source-path\": \"s3://$BUCKET/$DATA_PREFIX/\",
    \"--contract\": \"s3://$BUCKET/glue/contract.json\",
    \"--redis-host\": \"$REDIS_HOST\",
    \"--redis-port\": \"$REDIS_PORT\",
    \"--redis-password\": \"$REDIS_PASSWORD\",
    \"--create-index\": \"true\",
    \"--sample\": \"25\"
  }" --query JobRunId --output text) || exit 1
  echo "run id: $RUN_ID (watch it in the Glue console too)"

  while true; do
    STATE=$(aws glue get-job-run --job-name "$JOB" --run-id "$RUN_ID" --query JobRun.JobRunState --output text)
    echo "  state: $STATE"
    case "$STATE" in
      SUCCEEDED|FAILED|ERROR|TIMEOUT|STOPPED) break ;;
    esac
    sleep 20
  done

  echo ""
  echo "=== proof from Redis Cloud ==="
  docker run --rm redis:8.2 redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" --no-auth-warning DBSIZE
  docker run --rm redis:8.2 redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" --no-auth-warning HGETALL fs:customer-features:C000042
  docker run --rm redis:8.2 redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" --no-auth-warning \
    FT.AGGREGATE idx:fs:customer-features "*" GROUPBY 1 @segment REDUCE AVG 1 @fraud_score AS avg_fraud REDUCE COUNT 0 AS customers SORTBY 2 @avg_fraud DESC
  [ "$STATE" = "SUCCEEDED" ]
}

teardown() {
  aws glue delete-job --job-name "$JOB" 2>/dev/null && echo "job deleted"
  aws iam delete-role-policy --role-name "$ROLE" --policy-name s3-demo-bucket 2>/dev/null
  aws iam detach-role-policy --role-name "$ROLE" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole 2>/dev/null
  aws iam delete-role --role-name "$ROLE" 2>/dev/null && echo "role deleted"
  aws s3 rb "s3://$BUCKET" --force && echo "bucket deleted"
}

case "$ACTION" in
  setup) setup ;;
  run) run ;;
  teardown) teardown ;;
  *) echo "usage: $0 setup|run|teardown"; exit 2 ;;
esac
