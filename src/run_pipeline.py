"""Orchestrate the local synthetic ride lake. A run is not cross-table atomic."""

import argparse
import hashlib
import json
import math
import os
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import yaml
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

from ingest import ingest_csv
from partition_writer import merge_city_history, merge_rides
from transform import empty_city_history, prepare_bronze, prepare_gold_fact, prepare_silver, update_city_history
from validate import validate_records

ROOT = Path(__file__).resolve().parents[1]


def project_path(value):
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def load_config(path):
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def make_spark(config):
    builder = (SparkSession.builder.master(config["spark"]["master"])
               .appName(config["spark"]["app_name"])
               .config("spark.sql.session.timeZone", config["spark"]["session_timezone"])
               .config("spark.sql.shuffle.partitions", config["spark"].get("shuffle_partitions", 4))
               .config("spark.databricks.delta.snapshotPartitions", config["spark"].get("delta_snapshot_partitions", 4))
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_json(path, value):
    """Atomically replace a small local JSON report, not a Delta table."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         delete=False, suffix=".tmp") as handle:
            temporary = handle.name
            json.dump(value, handle, indent=2)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_instant(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Configured timestamps must include a timezone")
    return result.astimezone(timezone.utc)


def batch_fingerprint(path, validation_config):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    digest.update(json.dumps(validation_config, sort_keys=True).encode("utf-8"))
    digest.update(b"validation-contract-v1")
    return digest.hexdigest()


def table_fingerprint(df):
    """Practical order-independent digest for comparing rerun table contents.

    XOR and decimal sum of row hashes reduce accidental collisions but do not
    constitute a cryptographic proof of equality. Columns are sorted by name.
    """
    canonical = F.to_json(F.struct(*[F.col(name) for name in sorted(df.columns)]), {"ignoreNullFields": "false"})
    hashed = df.select(F.xxhash64(canonical).alias("row_hash"))
    result = hashed.agg(F.expr("bit_xor(row_hash)").alias("xor"),
                        F.sum(F.col("row_hash").cast("decimal(38,0)")).alias("sum")).first()
    return {"xor": str(result["xor"]), "sum": str(result["sum"])}


def execute_pipeline(spark, config, run_id, log):
    paths, rules = config["paths"], config["validation"]
    source = project_path(paths["source"])
    cutoff = parse_instant(rules["as_of_utc"])
    log["validation_as_of_utc"] = cutoff.isoformat()
    log["source_file"] = source.name
    raw = ingest_csv(spark, source, run_id).cache()
    log["input_count"] = raw.count()
    if log["input_count"] == 0:
        for name in ("valid_count", "rejected_count", "bronze_count", "silver_count", "gold_fact_count"):
            log[name] = 0
        return {"source_count": 0, "quarantine_count": 0, "bronze_batch_count": 0,
                "silver_batch_count": 0, "gold_batch_count": 0,
                "note": "Empty batch; existing tables were not changed."}

    valid, rejects = validate_records(raw, rules["allowed_cities"], rules["allowed_statuses"], cutoff)
    valid, rejects = valid.cache(), rejects.cache()
    log["valid_count"], log["rejected_count"] = valid.count(), rejects.count()
    if log["input_count"] != log["valid_count"] + log["rejected_count"]:
        raise ValueError("Source-to-validation counts do not reconcile")
    # One deterministic quarantine table per input content and validation rules.
    # Overwrite is atomic at the individual Delta table level; no cross-table transaction.
    fingerprint = batch_fingerprint(source, rules)
    log["batch_fingerprint"] = fingerprint
    quarantine_path = project_path(paths["quarantine"]) / fingerprint
    rejects.write.format("delta").mode("overwrite").save(str(quarantine_path))
    if not log["valid_count"]:
        for name in ("bronze_count", "silver_count", "gold_fact_count"):
            log[name] = 0
        return {"source_count": log["input_count"], "quarantine_count": log["rejected_count"],
                "bronze_batch_count": 0, "silver_batch_count": 0, "gold_batch_count": 0,
                "note": "All input records rejected; existing target tables were not changed."}

    bronze_path, silver_path = project_path(paths["bronze"]), project_path(paths["silver"])
    city_path, fact_path = project_path(paths["gold_cities"]), project_path(paths["gold_facts"])
    bronze_input = prepare_bronze(valid)
    merge_rides(bronze_input, bronze_path)
    bronze = spark.read.format("delta").load(str(bronze_path))
    merge_rides(prepare_silver(bronze), silver_path, timestamp_column="event_time")
    silver = spark.read.format("delta").load(str(silver_path))

    city_config = config["city_dimension"]
    city_snapshot = spark.createDataFrame(sorted(city_config["regions"].items()), ["city_name", "region"])
    if DeltaTable.isDeltaTable(spark, str(city_path)):
        old_history = spark.read.format("delta").load(str(city_path))
        effective = parse_instant(city_config["changes_effective_at"])
    else:
        old_history = empty_city_history(spark)
        effective = parse_instant(city_config["initial_valid_from"])
    # Materialize before MERGE so the snapshot does not read its own changing target.
    city_history = update_city_history(old_history, city_snapshot, effective).localCheckpoint(eager=True)
    merge_city_history(city_history, city_path)
    city_history = spark.read.format("delta").load(str(city_path))
    facts = prepare_gold_fact(silver, city_history)
    merge_rides(facts, fact_path, timestamp_column="event_time", partitioned=False)
    stored_facts = spark.read.format("delta").load(str(fact_path))

    keys = valid.select("ride_id").distinct()
    bronze_batch = bronze.join(keys, "ride_id", "inner").count()
    silver_batch = silver.join(keys, "ride_id", "inner").count()
    gold_batch = stored_facts.join(keys, "ride_id", "inner").count()
    log["bronze_count"], log["silver_count"], log["gold_fact_count"] = bronze_batch, silver_batch, gold_batch
    if log["input_count"] != log["rejected_count"] + bronze_batch:
        raise ValueError("Source vs quarantine plus Bronze batch does not reconcile")
    if bronze_batch != silver_batch or silver_batch != gold_batch:
        raise ValueError("Bronze, Silver and Gold batch counts do not reconcile")
    def fare_total(frame):
        value = (frame.join(keys, "ride_id", "inner")
                 .agg(F.sum(F.col("fare").cast("double")).alias("total")).first()["total"])
        return float(value or 0.0)
    bronze_fare, silver_fare, gold_fare = fare_total(bronze), fare_total(silver), fare_total(stored_facts)
    if not (math.isclose(bronze_fare, silver_fare, rel_tol=1e-9, abs_tol=1e-6) and
            math.isclose(silver_fare, gold_fare, rel_tol=1e-9, abs_tol=1e-6)):
        raise ValueError("Bronze, Silver and Gold batch fare totals do not reconcile")
    return {"source_count": log["input_count"], "quarantine_count": log["rejected_count"],
            "bronze_batch_count": bronze_batch, "silver_batch_count": silver_batch,
            "gold_batch_count": gold_batch, "bronze_total_count": bronze.count(),
            "silver_total_count": silver.count(), "gold_total_count": stored_facts.count(),
            "bronze_batch_fare": bronze_fare, "silver_batch_fare": silver_fare,
            "gold_batch_fare": gold_fare,
            "bronze_fingerprint": table_fingerprint(bronze),
            "silver_fingerprint": table_fingerprint(silver),
            "gold_fingerprint": table_fingerprint(stored_facts),
            "note": "Batch counts are joined on this input's validated ride IDs; totals include prior batches."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--label", choices=("first", "rerun"),
                        help="Optional readable report name for an isolated run")
    args = parser.parse_args()
    config = load_config(args.config)
    run_id = "RUN_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid4().hex[:8]
    filename = f"pipeline_{args.label}.json" if args.label else run_id + ".json"
    report_path = project_path(config["paths"]["reconciliation"]) / filename
    log_path = project_path(config["paths"]["run_logs"]) / filename
    if args.label and (report_path.exists() or log_path.exists()):
        raise FileExistsError(f"Report label already exists: {args.label}; use an isolated run directory")
    log = {"run_id": run_id, "start_time": utc_now(), "end_time": None, "input_count": None,
           "valid_count": None, "rejected_count": None, "bronze_count": None, "silver_count": None,
           "gold_fact_count": None, "status": "running", "failure_reason": None}
    spark = None
    try:
        spark = make_spark(config)
        report = execute_pipeline(spark, config, run_id, log)
        report["run_id"] = run_id
        write_json(report_path, report)
        log["status"] = "success"
        print(json.dumps(report, indent=2))
    except Exception as exc:
        log["status"], log["failure_reason"] = "failed", f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
        raise
    finally:
        log["end_time"] = utc_now()
        try:
            write_json(log_path, log)
        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
