#!/usr/bin/env python3
"""
Hydrate Redis from a SageMaker Feature Store OFFLINE store (S3, Parquet).

The offline store is append-only: it holds every version of every record plus
DeleteRecord tombstones (is_deleted=True). This loader materializes the online
view the same way AWS documents it for Athena:

  latest record per record identifier (ORDER BY event_time DESC, write_time DESC)
  filtered by is_deleted = false

Then it writes to Redis (hash by default, JSON optional), pipelined, and
validates itself: key count plus a random sample read back and compared
field by field. Exit code is non-zero if anything does not match.
"""

import argparse
import io
import json
import logging
import os
import random
import sys
from datetime import datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args():
    ap = argparse.ArgumentParser(description="Hydrate Redis from a Feature Store offline store (S3 Parquet).")
    ap.add_argument("--bucket", default=os.getenv("BUCKET", "sagemaker-offline-store-demo"))
    ap.add_argument("--data-prefix", default=os.getenv("DATA_PREFIX",
                    "feature-store/111122223333/sagemaker/us-east-1/offline-store/customer-features-1755600000/data"),
                    help="S3 prefix of the feature group's data/ directory (from ResolvedOutputS3Uri).")
    ap.add_argument("--record-id-col", default="customer_id")
    ap.add_argument("--event-time-col", default="event_time")
    ap.add_argument("--write-time-col", default="write_time")
    ap.add_argument("--deleted-col", default="is_deleted")
    ap.add_argument("--target", choices=["hash", "json"], default="hash")
    ap.add_argument("--key-prefix", default="fs:customer-features")
    ap.add_argument("--ttl", type=int, default=0, help="Optional TTL in seconds for every key (0 = no TTL).")
    ap.add_argument("--sample", type=int, default=25, help="Random entities to read back and verify.")
    ap.add_argument("--keep-metadata", action="store_true",
                    help="Also write write_time/api_invocation_time/is_deleted fields.")
    ap.add_argument("--export-snapshot", default="",
                    help="Optional S3 prefix to also write the deduped snapshot as one Parquet file "
                         "(input for riotx file-import or audits).")
    return ap.parse_args()


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def list_parquet_keys(s3, bucket, prefix):
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys.extend(o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet"))
        if not resp.get("IsTruncated"):
            return keys
        token = resp["NextContinuationToken"]


def load_rows(s3, bucket, keys):
    rows = []
    for key in keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        rows.extend(table.to_pylist())
    return rows


def dedupe_latest(rows, id_col, event_col, write_col, deleted_col):
    """AWS-documented snapshot semantics: latest per record id by
    (event_time, write_time), then drop tombstoned entities."""
    rows.sort(key=lambda r: (str(r[event_col]), r[write_col] or datetime.min))
    latest = {}
    for r in rows:
        latest[r[id_col]] = r  # ascending sort: last one wins
    live = {cid: r for cid, r in latest.items() if not r.get(deleted_col)}
    tombstoned = len(latest) - len(live)
    return live, tombstoned


def to_document(row, args):
    metadata_cols = {args.write_time_col, "api_invocation_time", args.deleted_col}
    doc = {}
    for k, v in row.items():
        if not args.keep_metadata and k in metadata_cols:
            continue
        if isinstance(v, datetime):
            v = v.isoformat()
        doc[k] = v
    return doc


def to_hash_mapping(doc):
    mapping = {}
    for k, v in doc.items():
        if v is None:
            continue
        if isinstance(v, bool):
            mapping[k] = "true" if v else "false"
        else:
            mapping[k] = str(v)
    return mapping


def export_snapshot(s3, bucket, prefix, docs):
    table = pa.Table.from_pylist(docs)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    stamp = "snapshot"
    key = f"{prefix.rstrip('/')}/{stamp}.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info("Snapshot exported: s3://%s/%s (%d records)", bucket, key, len(docs))
    return key


def main():
    args = parse_args()
    s3 = s3_client()
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6398")),
        db=int(os.getenv("REDIS_DB", "0")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    r.ping()

    keys = list_parquet_keys(s3, args.bucket, args.data_prefix)
    if not keys:
        logger.error("No parquet files under s3://%s/%s", args.bucket, args.data_prefix)
        sys.exit(2)
    logger.info("Found %d parquet files under s3://%s/%s", len(keys), args.bucket, args.data_prefix)

    rows = load_rows(s3, args.bucket, keys)
    logger.info("Loaded %d raw records (append-only history)", len(rows))

    live, tombstoned = dedupe_latest(rows, args.record_id_col, args.event_time_col,
                                     args.write_time_col, args.deleted_col)
    logger.info("Deduped to %d live entities (%d tombstoned entities dropped)", len(live), tombstoned)

    docs = {cid: to_document(row, args) for cid, row in live.items()}

    written = 0
    pipe = r.pipeline(transaction=False)
    for cid, doc in docs.items():
        key = f"{args.key_prefix}:{cid}"
        if args.target == "hash":
            pipe.hset(key, mapping=to_hash_mapping(doc))
        else:
            pipe.json().set(key, "$", doc)
        if args.ttl > 0:
            pipe.expire(key, args.ttl)
        written += 1
        if written % 500 == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            logger.info("Written %d/%d ...", written, len(docs))
    pipe.execute()
    logger.info("Hydration complete: %d keys written as %s under '%s:*'", written, args.target.upper(), args.key_prefix)

    # Validation 1: key count under the prefix
    count, cursor = 0, 0
    while True:
        cursor, page = r.scan(cursor, match=f"{args.key_prefix}:*", count=1000)
        count += len(page)
        if cursor == 0:
            break
    counts_ok = count == len(docs)
    logger.info("%s Key count check: expected=%d redis=%d",
                "PASS" if counts_ok else "FAIL", len(docs), count)

    # Validation 2: random sample content read-back
    mismatches = 0
    sample_ids = random.sample(sorted(docs), min(args.sample, len(docs)))
    for cid in sample_ids:
        key = f"{args.key_prefix}:{cid}"
        if args.target == "hash":
            stored = r.hgetall(key)
            expected = to_hash_mapping(docs[cid])
        else:
            res = r.json().get(key, "$")
            stored = res[0] if isinstance(res, list) and res else res
            expected = docs[cid]
        if stored != expected:
            mismatches += 1
            logger.warning("Sample mismatch for %s", key)
    sample_ok = mismatches == 0
    logger.info("%s Sample content check: %d/%d entities match",
                "PASS" if sample_ok else "FAIL", len(sample_ids) - mismatches, len(sample_ids))

    if args.export_snapshot:
        export_snapshot(s3, args.bucket, args.export_snapshot, list(docs.values()))

    if not (counts_ok and sample_ok):
        logger.error("Hydration finished WITH PROBLEMS. Exiting non-zero.")
        sys.exit(1)
    logger.info("Hydration finished cleanly.")


if __name__ == "__main__":
    main()
