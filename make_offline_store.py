#!/usr/bin/env python3
"""
Generates a realistic SageMaker Feature Store OFFLINE store in S3 (MinIO).

Reproduces the documented AWS Glue table format layout exactly:
  s3://{bucket}/{prefix}/{account}/sagemaker/{region}/offline-store/
      {feature-group}-{creation-ts}/data/year=YYYY/month=MM/day=DD/hour=HH/
      {timestamp}_{16-random-alnum}.parquet

Reproduces offline store semantics:
- Append-only: multiple versions per record identifier across partitions
- Extra metadata columns appended by Feature Store:
  write_time, api_invocation_time, is_deleted
- DeleteRecord tombstones: newest record for some entities has is_deleted=True

Deterministic (seeded) so the demo is reproducible.
"""

import io
import os
import random
import string
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)

# Default targets local MinIO. Set S3_ENDPOINT_URL=aws (or empty) for real S3.
ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9002")
if ENDPOINT in ("", "aws"):
    ENDPOINT = None
BUCKET = os.getenv("BUCKET", "sagemaker-offline-store-demo")
ACCOUNT = "111122223333"
REGION = os.getenv("AWS_REGION", "us-east-1")
FEATURE_GROUP = "customer-features"
FG_CREATION_TS = "1755600000"
BASE_PREFIX = (
    f"feature-store/{ACCOUNT}/sagemaker/{REGION}/offline-store/"
    f"{FEATURE_GROUP}-{FG_CREATION_TS}/data"
)

N_ENTITIES = 1000
N_DELETED = 30
SEGMENTS = ["retail", "premium", "private", "digital"]

if ENDPOINT:
    # MinIO: explicit demo credentials
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )
else:
    # Real AWS: default credential chain (env, ~/.aws, instance role)
    s3 = boto3.client("s3", region_name=REGION)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def rand_suffix(n=16):
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def make_version(cid: str, event_dt: datetime, deleted=False):
    """One offline store row: features + the metadata columns Feature Store appends."""
    write_dt = event_dt + timedelta(minutes=random.randint(5, 14))  # PutRecord buffers up to 15 min
    return {
        "customer_id": cid,
        "event_time": iso(event_dt),
        "fraud_score": round(random.random(), 6),
        "credit_score": random.randint(300, 1000),
        "avg_ticket_30d": round(random.uniform(20, 5000), 2),
        "tx_count_24h": random.randint(0, 120),
        "device_trust": round(random.random(), 4),
        "segment": random.choice(SEGMENTS),
        "is_pep": random.random() < 0.02,
        # metadata columns appended by Feature Store
        "api_invocation_time": event_dt + timedelta(seconds=1),
        "write_time": write_dt,
        "is_deleted": deleted,
    }


SCHEMA = pa.schema([
    ("customer_id", pa.string()),
    ("event_time", pa.string()),
    ("fraud_score", pa.float64()),
    ("credit_score", pa.int64()),
    ("avg_ticket_30d", pa.float64()),
    ("tx_count_24h", pa.int64()),
    ("device_trust", pa.float64()),
    ("segment", pa.string()),
    ("is_pep", pa.bool_()),
    ("api_invocation_time", pa.timestamp("ms", tz="UTC")),
    ("write_time", pa.timestamp("ms", tz="UTC")),
    ("is_deleted", pa.bool_()),
])


def upload_parquet(rows, hour_dt: datetime):
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    key = (
        f"{BASE_PREFIX}/year={hour_dt.year}/month={hour_dt.month:02d}/"
        f"day={hour_dt.day:02d}/hour={hour_dt.hour:02d}/"
        f"{hour_dt.strftime('%Y%m%dT%H%M%SZ')}_{rand_suffix()}.parquet"
    )
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return key


def main():
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(Bucket=BUCKET,
                             CreateBucketConfiguration={"LocationConstraint": REGION})
    except Exception:
        pass  # already exists

    base = datetime(2026, 8, 19, 12, 0, 0, tzinfo=timezone.utc)
    hours = [base, base + timedelta(hours=1), base + timedelta(hours=2)]
    entities = [f"C{i:06d}" for i in range(N_ENTITIES)]
    deleted_ids = set(random.sample(entities, N_DELETED))

    # Build versions: every entity gets 1-3 rows spread over the 3 hourly
    # partitions; deleted entities get a final tombstone in the last hour.
    per_hour_rows = {0: [], 1: [], 2: []}
    total_rows = 0
    for cid in entities:
        n_versions = random.randint(1, 3)
        hour_idxs = sorted(random.sample(range(3), n_versions))
        for hi in hour_idxs:
            event_dt = hours[hi] + timedelta(
                minutes=random.randint(0, 44), seconds=random.randint(0, 59),
                microseconds=random.randint(0, 999) * 1000,
            )
            per_hour_rows[hi].append(make_version(cid, event_dt))
            total_rows += 1
        if cid in deleted_ids:
            event_dt = hours[2] + timedelta(minutes=random.randint(45, 59))
            per_hour_rows[2].append(make_version(cid, event_dt, deleted=True))
            total_rows += 1

    # Two parquet files per hourly partition, like real multi-writer output
    n_objects = 0
    for hi, rows in per_hour_rows.items():
        random.shuffle(rows)
        half = len(rows) // 2
        for chunk in (rows[:half], rows[half:]):
            if chunk:
                key = upload_parquet(chunk, hours[hi])
                n_objects += 1
                print(f"[offline-store] wrote s3://{BUCKET}/{key}  ({len(chunk)} records)")

    print(f"\n[offline-store] done: {total_rows} records, {n_objects} parquet files, "
          f"{N_ENTITIES} entities, {N_DELETED} tombstoned")
    print(f"[offline-store] expected live entities after dedupe: {N_ENTITIES - N_DELETED}")
    print(f"[offline-store] ResolvedOutputS3Uri equivalent: s3://{BUCKET}/{BASE_PREFIX[:-5]}")


if __name__ == "__main__":
    main()
