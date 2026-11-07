"""Demonstrate late-arrival partitioning and SCD2 using isolated Delta tables."""

import argparse
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

from pyspark.sql import functions as F

from ingest import RAW_SCHEMA
from partition_writer import merge_city_history, merge_rides
from run_pipeline import load_config, make_spark, parse_instant, project_path, write_json
from transform import empty_city_history, prepare_bronze, prepare_gold_fact, update_city_history
from validate import validate_records


def run_demo(spark, config):
    """Create fresh demo tables; never modify the pipeline's main tables."""
    root = project_path(config["paths"]["reviewer_demo"])
    if root.exists():
        raise FileExistsError(f"Demo path already exists: {root}; use a new path")
    rides_path, city_path = root / "late_rides", root / "city_history"
    rules = config["validation"]
    cutoff = parse_instant(rules["as_of_utc"])

    def make_valid(rows, batch):
        raw = (spark.createDataFrame(rows, RAW_SCHEMA)
               .withColumn("run_id", F.lit(batch)).withColumn("source_file", F.lit(batch + ".csv")))
        valid, rejected = validate_records(raw, rules["allowed_cities"], rules["allowed_statuses"], cutoff)
        if valid.count() != len(rows) or rejected.count() != 0:
            raise ValueError("Demo record failed validation; inspect the validation cutoff")
        return prepare_bronze(valid)

    anchor = ("DEMO_HISTORICAL", "RIDER_DEMO", "DRIVER_DEMO", "Mumbai", "80.00",
              "2024-02-29T09:00:00Z", "completed", None)
    first = ("DEMO_FIRST", "RIDER_DEMO", "DRIVER_DEMO", "Mumbai", "120.00",
             "2024-03-04T10:00:00Z", "completed", None)
    late = ("DEMO_LATE", "RIDER_DEMO", "DRIVER_DEMO", "Mumbai", "90.00",
            "2024-02-29T10:00:00Z", "completed", None)
    merge_rides(make_valid([anchor, first], "batch_01"), rides_path)
    before = spark.read.format("delta").load(str(rides_path))
    before_count = before.count()
    historical_before = before.filter((F.col("year") == 2024) & (F.col("month") == 2) & (F.col("day") == 29)).count()
    merge_rides(make_valid([late], "batch_02"), rides_path)
    stored = spark.read.format("delta").load(str(rides_path))
    after_count = stored.count()
    historical_after = stored.filter((F.col("year") == 2024) & (F.col("month") == 2) & (F.col("day") == 29)).count()
    late_rows = (stored.filter(F.col("ride_id") == "DEMO_LATE")
                 .select("ride_id", "event_time", "year", "month", "day",
                         F.input_file_name().alias("data_file")).collect())
    if (len(late_rows) != 1 or before_count != 2 or after_count != 3 or
            historical_before != 1 or historical_after != 2):
        raise ValueError("Late-arrival demo did not preserve both batches")
    result = late_rows[0]
    expected_parts = ["year=2024", "month=2", "day=29"]
    file_parts = unquote(result["data_file"]).replace("\\", "/").split("/")
    if [result["year"], result["month"], result["day"]] != [2024, 2, 29] or not all(p in file_parts for p in expected_parts):
        raise ValueError("Late record was not found in its event-date partition")
    late_report = {
        "scenario": "A second batch adds an earlier event to an existing historical partition.",
        "existing_historical_event_time": anchor[5],
        "first_batch_event_time": first[5], "late_batch_event_time": late[5],
        "rows_before_second_batch": before_count, "rows_after_second_batch": after_count,
        "historical_partition_rows_before": historical_before,
        "historical_partition_rows_after": historical_after,
        "late_ride_id": result["ride_id"], "observed_partition": "/".join(expected_parts),
        "partition_verified_from": "Delta data-file path and stored partition columns",
        "note": "Two separate writes to an isolated demo table; no pipeline tables are modified."
    }

    initial = spark.createDataFrame([("Mumbai", "West")], ["city_name", "region"])
    first_history = update_city_history(empty_city_history(spark), initial,
                                        datetime(2023, 1, 1, tzinfo=timezone.utc)).localCheckpoint(eager=True)
    merge_city_history(first_history, city_path)
    before_history = spark.read.format("delta").load(str(city_path))

    def readable(frame):
        return (frame.select("city_name", "region", "is_current",
                             F.date_format("valid_from", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("valid_from_utc"),
                             F.date_format("valid_to", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("valid_to_utc"))
                .orderBy("valid_from_utc"))

    print("\nCITY DIMENSION BEFORE CHANGE")
    readable(before_history).show(truncate=False)
    before = [row.asDict() for row in readable(before_history).collect()]
    changed = spark.createDataFrame([("Mumbai", "New West")], ["city_name", "region"])
    next_history = update_city_history(before_history, changed,
                                       datetime(2025, 1, 1, tzinfo=timezone.utc)).localCheckpoint(eager=True)
    merge_city_history(next_history, city_path)
    history = spark.read.format("delta").load(str(city_path))
    print("\nCITY DIMENSION AFTER CHANGE")
    readable(history).show(truncate=False)
    after = [row.asDict() for row in readable(history).collect()]
    if len(before) != 1 or len(after) != 2 or {r["region"]: r["is_current"] for r in after} != {"West": False, "New West": True}:
        raise ValueError("SCD Type 2 demo did not preserve the expected versions")
    old = next(row for row in after if row["region"] == "West")
    if old["valid_to_utc"] != "2025-01-01T00:00:00Z":
        raise ValueError("Previous city version closed at an unexpected time")

    sample = (spark.createDataFrame([
        ("DEMO_OLD", "RIDER_DEMO", "DRIVER_DEMO", "Mumbai", 120.0, "2024-06-01T00:00:00Z"),
        ("DEMO_NEW", "RIDER_DEMO", "DRIVER_DEMO", "Mumbai", 130.0, "2025-06-01T00:00:00Z"),
    ], ["ride_id", "rider_id", "driver_id", "city", "fare", "event_time"])
              .withColumn("event_time", F.to_timestamp("event_time")))
    mapped = (prepare_gold_fact(sample, history).join(history.select("city_key", "region"), "city_key")
              .select("ride_id", "region").orderBy("ride_id"))
    print("\nRIDE-TO-CITY VERSION MAPPING")
    mapped.show(truncate=False)
    mappings = [row.asDict() for row in mapped.collect()]
    if {row["ride_id"]: row["region"] for row in mappings} != {"DEMO_OLD": "West", "DEMO_NEW": "New West"}:
        raise ValueError("Rides did not resolve to the expected historical city versions")
    city_report = {"scenario": "A controlled Mumbai region change in an isolated city table.",
                   "effective_at_utc": "2025-01-01T00:00:00Z", "before": before,
                   "after": after, "ride_version_mapping": mappings,
                   "note": "Synthetic example; region names are illustrative."}
    print("\nLATE ARRIVAL: FIRST BATCH vs SECOND BATCH")
    print(f"{'First batch event':<28} {first[5]}")
    print(f"{'Late batch event':<28} {late[5]}")
    print(f"{'Observed late partition':<28} {late_report['observed_partition']}")
    print(f"{'Rows before / after':<28} {before_count} / {after_count}")
    print(f"{'Historical rows before / after':<28} {historical_before} / {historical_after}")
    return late_report, city_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.yaml"))
    args = parser.parse_args()
    config = load_config(args.config)
    report_dir = project_path(config["paths"]["reconciliation"])
    late_path, city_path = report_dir / "late_arrival.json", report_dir / "city_history.json"
    if late_path.exists() or city_path.exists():
        raise FileExistsError("Reviewer demo reports already exist; use a new output directory")
    spark = make_spark(config)
    try:
        late_report, city_report = run_demo(spark, config)
        write_json(late_path, late_report)
        write_json(city_path, city_report)
        print("\nDemo reports:", late_path, city_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
