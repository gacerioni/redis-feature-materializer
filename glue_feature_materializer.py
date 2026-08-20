#!/usr/bin/env python3
"""
Reference Glue job: materialize batch features (S3 Parquet) into Redis.

This is the blessed pattern for feature platforms that already run Spark/Glue:
read Parquet, resolve the latest version per entity, drop tombstones, write
Redis hashes in parallel from the executors, create the search index from the
feature contract, then validate and exit non-zero on any mismatch.

Runs unchanged on AWS Glue (submit as a Spark job) and on plain spark-submit.
Connection math is explicit: each Spark partition opens ONE Redis connection,
so total connections = min(shuffle partitions, executors x cores). The Redis
Enterprise proxy multiplexes these; the cap exists to keep the client side
predictable, not to protect Redis.

Feature contract (--contract, JSON): mirrors the platform's feature group
metadata (what the team already keeps in YAML):
{
  "feature_group": "customer-features",
  "entity_column": "customer_id",
  "event_time_column": "event_time",       # "" disables dedupe
  "write_time_column": "write_time",       # tie-breaker, optional
  "deleted_column": "is_deleted",          # optional tombstone flag
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
}
"""

import argparse
import json
import logging
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("feature-materializer")

PIPELINE_BATCH = 500


def parse_args(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-path", required=True, help="s3://... or s3a://... path of the Parquet data")
    ap.add_argument("--contract", required=True,
                    help="Feature contract: inline JSON string, a local file path, or an s3:// URI")
    ap.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    ap.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    ap.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD", ""))
    ap.add_argument("--redis-ssl", action="store_true", help="Connect to Redis over TLS")
    ap.add_argument("--create-index", action="store_true",
                    help="Create the RediSearch index for the feature group if missing")
    ap.add_argument("--sample", type=int, default=25, help="Entities to read back and verify")
    # parse_known_args: AWS Glue injects its own arguments (--JOB_NAME,
    # --job-bookmark-option, ...) that must not crash the parser
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        logger.info("Ignoring %d extra arguments from the runtime (e.g. Glue): %s",
                    len(unknown), unknown[:6])
    return args


def load_contract(raw: str) -> dict:
    if raw.strip().startswith("{"):
        return json.loads(raw)
    if raw.startswith("s3://"):
        import boto3
        bucket, key = raw[5:].split("/", 1)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)
    with open(raw) as fh:
        return json.load(fh)


def normalize(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def make_writer(contract, host, port, password, ssl=False):
    """Returns the foreachPartition function. One connection per partition."""
    entity_col = contract["entity_column"]
    key_prefix = contract["key_prefix"]
    ttl = int(contract.get("ttl_seconds", 0))
    columns = [entity_col] + \
        ([contract["event_time_column"]] if contract.get("event_time_column") else []) + \
        list(contract["features"].keys())

    def write_partition(rows):
        import redis  # imported on the executor
        client = redis.Redis(host=host, port=port, password=password or None,
                             ssl=ssl, decode_responses=True, socket_timeout=10)
        pipe = client.pipeline(transaction=False)
        n = 0
        for row in rows:
            data = row.asDict()
            mapping = {}
            for col in columns:
                v = normalize(data.get(col))
                if v is not None:
                    mapping[col] = v
            key = f"{key_prefix}:{data[entity_col]}"
            pipe.hset(key, mapping=mapping)
            if ttl > 0:
                pipe.expire(key, ttl)
            n += 1
            if n % PIPELINE_BATCH == 0:
                pipe.execute()
        pipe.execute()
        client.close()

    return write_partition


def dedupe_latest(df, contract):
    """Latest version per entity, tombstones dropped. Same semantics AWS
    documents for Feature Store offline snapshots."""
    event_col = contract.get("event_time_column") or ""
    if not event_col:
        return df
    order = [F.col(event_col).desc()]
    write_col = contract.get("write_time_column") or ""
    if write_col and write_col in df.columns:
        order.append(F.col(write_col).desc())
    w = Window.partitionBy(contract["entity_column"]).orderBy(*order)
    df = df.withColumn("__rn", F.row_number().over(w)).filter(F.col("__rn") == 1).drop("__rn")
    deleted_col = contract.get("deleted_column") or ""
    if deleted_col and deleted_col in df.columns:
        df = df.filter(~F.coalesce(F.col(deleted_col), F.lit(False)))
    return df


def create_index(client, contract):
    import redis
    index_name = f"idx:{contract['key_prefix']}"
    schema_parts = []
    for field, ftype in contract["features"].items():
        schema_parts.extend([field, ftype])
    entity = contract["entity_column"]
    try:
        client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH",
            "PREFIX", "1", f"{contract['key_prefix']}:",
            "SCHEMA", entity, "TAG", *schema_parts,
        )
        logger.info("Created search index %s", index_name)
    except redis.exceptions.ResponseError as e:
        if "already exists" in str(e).lower():
            logger.info("Search index %s already exists", index_name)
        else:
            raise
    return index_name


def validate(client, df, contract, sample_size):
    key_prefix = contract["key_prefix"]
    entity_col = contract["entity_column"]
    expected = df.count()

    count, cursor = 0, 0
    while True:
        cursor, page = client.scan(cursor, match=f"{key_prefix}:*", count=1000)
        count += len(page)
        if cursor == 0:
            break
    counts_ok = count == expected
    logger.info("%s Key count check: expected=%d redis=%d",
                "PASS" if counts_ok else "FAIL", expected, count)

    mismatches = 0
    feature_cols = list(contract["features"].keys())
    for row in df.limit(sample_size).collect():
        data = row.asDict()
        stored = client.hgetall(f"{key_prefix}:{data[entity_col]}")
        for col in feature_cols:
            expected_v = normalize(data.get(col))
            if expected_v is not None and stored.get(col) != expected_v:
                mismatches += 1
                logger.warning("Sample mismatch %s field %s: expected=%r stored=%r",
                               data[entity_col], col, expected_v, stored.get(col))
                break
    sample_ok = mismatches == 0
    logger.info("%s Sample content check: %d/%d entities match",
                "PASS" if sample_ok else "FAIL", sample_size - mismatches, sample_size)
    return counts_ok and sample_ok


def main(argv=None):
    args = parse_args(argv)
    contract = load_contract(args.contract)

    spark = SparkSession.builder.appName(f"feature-materializer-{contract['feature_group']}").getOrCreate()

    df = spark.read.parquet(args.source_path)
    raw_count = df.count()
    df = dedupe_latest(df, contract)
    live_count = df.count()
    logger.info("Read %d raw records, %d live entities after dedupe", raw_count, live_count)

    needed = [contract["entity_column"]] + \
        ([contract["event_time_column"]] if contract.get("event_time_column") else []) + \
        list(contract["features"].keys())
    df = df.select(*[c for c in needed if c in df.columns]).cache()

    writer = make_writer(contract, args.redis_host, args.redis_port, args.redis_password,
                         ssl=args.redis_ssl)
    partitions = df.rdd.getNumPartitions()
    logger.info("Writing with %d partitions (= max concurrent Redis connections)", partitions)
    df.rdd.foreachPartition(writer)
    logger.info("Write phase done: %d entities under '%s:*'", live_count, contract["key_prefix"])

    import redis
    client = redis.Redis(host=args.redis_host, port=args.redis_port,
                         password=args.redis_password or None, ssl=args.redis_ssl,
                         decode_responses=True)
    if args.create_index:
        create_index(client, contract)

    ok = validate(client, df, contract, min(args.sample, live_count))
    spark.stop()
    if not ok:
        logger.error("Materialization finished WITH PROBLEMS. Exiting non-zero.")
        sys.exit(1)
    logger.info("Materialization finished cleanly.")


if __name__ == "__main__":
    main()
