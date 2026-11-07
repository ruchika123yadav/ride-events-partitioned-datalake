"""Spark and Delta pipeline checks."""

import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingest import RAW_SCHEMA, SOURCE_FIELDS, ingest_csv
from generate_data import generate_rows
from partition_writer import merge_city_history, merge_rides
from optimize import run_demo as run_compaction_demo
from prune_demo import run_demo as run_pruning_demo
import run_pipeline
from transform import empty_city_history, prepare_bronze, prepare_gold_fact, prepare_silver, update_city_history
from validate import validate_records

CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"]
STATUSES = ["completed", "cancelled", "in_progress"]
CUTOFF = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def spark():
    builder = (SparkSession.builder.master("local[2]").appName("ride-lake-tests")
               .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.databricks.delta.snapshotPartitions", "1")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def row(ride_id="r1", city="Mumbai", fare="100.00", event_time="2024-04-04T10:00:00Z", corrupt=None):
    return (ride_id, "u1", "d1", city, fare, event_time, "completed", corrupt)


def raw_df(spark, rows):
    return (spark.createDataFrame(rows, RAW_SCHEMA)
            .withColumn("run_id", F.lit("test_run"))
            .withColumn("source_file", F.lit("test_input.csv")))


def split(df):
    return validate_records(df, CITIES, STATUSES, CUTOFF)


def reasons(df):
    return [r["rejection_reason"] for r in df.select("rejection_reason").collect()]


def test_happy_path_to_gold(spark, tmp_path):
    good, bad = split(raw_df(spark, [row()]))
    assert bad.count() == 0
    bronze = prepare_bronze(good)
    silver = prepare_silver(bronze)
    changes = spark.createDataFrame([("Mumbai", "West")], ["city_name", "region"])
    history = update_city_history(empty_city_history(spark), changes, datetime(2023, 1, 1, tzinfo=timezone.utc)).cache()
    assert history.count() == 1
    fact = prepare_gold_fact(silver, history)
    assert fact.count() == 1
    merge_rides(bronze, tmp_path / "bronze")
    merge_city_history(history, tmp_path / "city")
    merge_rides(fact, tmp_path / "fact", timestamp_column="event_time", partitioned=False)
    assert spark.read.format("delta").load(str(tmp_path / "fact")).count() == 1


def test_empty_batch(spark, tmp_path):
    good, bad = split(raw_df(spark, []))
    assert good.count() == bad.count() == 0
    assert merge_rides(prepare_bronze(good), tmp_path / "empty") is False


def test_malformed_record_quarantined(spark, tmp_path):
    source = tmp_path / "malformed.csv"
    source.write_text(",".join(SOURCE_FIELDS) + "\nr1,u1,d1,Mumbai,100,2024-01-01T00:00:00Z,completed,extra\n", encoding="utf-8")
    good, bad = split(ingest_csv(spark, source, "malformed_run"))
    assert good.count() == 0
    item = bad.select("rejection_reason", "_corrupt_record", "source_file", "run_id").first()
    assert item["rejection_reason"] == "malformed_record" and item["_corrupt_record"]
    assert item["run_id"] == "malformed_run" and item["source_file"].endswith("malformed.csv")


def test_duplicate_key(spark):
    good, bad = split(raw_df(spark, [row(), row()]))
    assert good.count() == 1 and reasons(bad) == ["duplicate_ride_id"]


def test_missing_required_field(spark):
    good, bad = split(raw_df(spark, [row(event_time=None)]))
    assert good.count() == 0 and reasons(bad) == ["missing_event_time"]


def test_late_event_uses_event_date(spark):
    good, bad = split(raw_df(spark, [row(event_time="2024-02-29T23:50:00Z")]))
    bronze = prepare_bronze(good)
    partition = bronze.select("year", "month", "day").first()
    assert (partition.year, partition.month, partition.day) == (2024, 2, 29)
    assert bad.count() == 0


def test_late_batch_updates_existing_historical_partition(spark, tmp_path):
    target = tmp_path / "bronze"
    first, rejected = split(raw_df(spark, [
        row("historical", event_time="2024-02-29T09:00:00Z"),
        row("newer", event_time="2024-03-04T10:00:00Z"),
    ]))
    assert rejected.count() == 0
    merge_rides(prepare_bronze(first), target)
    before = spark.read.format("delta").load(str(target))
    historical = (F.col("year") == 2024) & (F.col("month") == 2) & (F.col("day") == 29)
    assert before.filter(historical).count() == 1

    later, rejected_later = split(raw_df(spark, [row("late", event_time="2024-02-29T10:00:00Z")]))
    assert rejected_later.count() == 0
    merge_rides(prepare_bronze(later), target)
    after = spark.read.format("delta").load(str(target))
    assert after.count() == 3
    assert after.filter(historical).count() == 2
    assert after.filter((F.col("ride_id") == "late") & historical).count() == 1


def test_invalid_city(spark):
    good, bad = split(raw_df(spark, [row(city="Atlantis")]))
    assert good.count() == 0 and reasons(bad) == ["invalid_city"]


def test_rerun_is_idempotent(spark, tmp_path):
    good, _ = split(raw_df(spark, [row()]))
    bronze = prepare_bronze(good)
    target = tmp_path / "rides"
    merge_rides(bronze, target)
    merge_rides(bronze.withColumn("run_id", F.lit("second_run")), target)
    assert spark.read.format("delta").load(str(target)).count() == 1


def test_future_timestamp_and_invalid_fare(spark):
    good, bad = split(raw_df(spark, [row("future", event_time="2027-01-01T00:00:00Z"), row("fare", fare="0")]))
    assert good.count() == 0
    assert sorted(reasons(bad)) == ["future_timestamp", "invalid_fare"]


def test_unexpected_header_fails(spark, tmp_path):
    path = tmp_path / "changed.csv"
    path.write_text("ride_id,unexpected_column\nr1,x\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Unexpected CSV header"):
        ingest_csv(spark, path, "schema_test")


def test_scd2_and_late_fact_mapping(spark):
    initial = spark.createDataFrame([("Mumbai", "West")], ["city_name", "region"])
    history = update_city_history(empty_city_history(spark), initial, datetime(2023, 1, 1, tzinfo=timezone.utc)).cache()
    history.count()
    changed = spark.createDataFrame([("Mumbai", "New West")], ["city_name", "region"])
    history = update_city_history(history, changed, datetime(2025, 1, 1, tzinfo=timezone.utc)).cache()
    assert history.count() == 2
    assert update_city_history(history, changed, datetime(2025, 1, 1, tzinfo=timezone.utc)).count() == 2
    rows = [row("old", event_time="2024-06-01T00:00:00Z"), row("new", event_time="2025-06-01T00:00:00Z")]
    good, _ = split(raw_df(spark, rows))
    facts = prepare_gold_fact(prepare_silver(prepare_bronze(good)), history)
    mapping = {r["ride_id"]: r["region"] for r in facts.join(history, "city_key").select("ride_id", "region").collect()}
    assert mapping == {"old": "West", "new": "New West"}


def test_partial_failure_retry(spark, tmp_path):
    good, _ = split(raw_df(spark, [row("r1"), row("r2")]))
    bronze = prepare_bronze(good)
    path = tmp_path / "bronze"
    merge_rides(bronze, path)  # Simulate failure after Bronze has committed.
    merge_rides(bronze, path)  # A retry must preserve both rows without duplicates.
    assert spark.read.format("delta").load(str(path)).count() == 2


def test_pipeline_recovers_after_injected_silver_failure(spark, tmp_path, monkeypatch):
    source = tmp_path / "rides.csv"
    source.write_text(",".join(SOURCE_FIELDS) + "\nr1,u1,d1,Mumbai,100,2024-04-04T10:00:00Z,completed\n", encoding="utf-8")
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    config = {
        "paths": {"source": str(source), "bronze": str(bronze), "silver": str(silver),
                  "quarantine": str(tmp_path / "quarantine"), "gold_cities": str(tmp_path / "cities"),
                  "gold_facts": str(tmp_path / "facts")},
        "validation": {"allowed_cities": CITIES, "allowed_statuses": STATUSES,
                       "as_of_utc": "2026-01-01T00:00:00Z"},
        "city_dimension": {"initial_valid_from": "2023-01-01T00:00:00Z",
                           "changes_effective_at": "2025-01-01T00:00:00Z",
                           "regions": {"Mumbai": "West"}},
    }
    original_merge = run_pipeline.merge_rides

    def fail_before_silver(df, destination, *args, **kwargs):
        if Path(destination) == silver:
            raise RuntimeError("injected failure before Silver commit")
        return original_merge(df, destination, *args, **kwargs)

    monkeypatch.setattr(run_pipeline, "merge_rides", fail_before_silver)
    with pytest.raises(RuntimeError, match="injected failure"):
        run_pipeline.execute_pipeline(spark, config, "failed_attempt", {})
    assert spark.read.format("delta").load(str(bronze)).count() == 1
    assert not silver.exists()

    monkeypatch.setattr(run_pipeline, "merge_rides", original_merge)
    report = run_pipeline.execute_pipeline(spark, config, "retry_attempt", {})
    assert report["bronze_batch_count"] == report["silver_batch_count"] == report["gold_batch_count"] == 1
    assert spark.read.format("delta").load(str(bronze)).count() == 1


def test_conflicting_replay_is_not_silently_overwritten(spark, tmp_path):
    original, _ = split(raw_df(spark, [row("r1", fare="100")]))
    changed, _ = split(raw_df(spark, [row("r1", fare="200")]))
    path = tmp_path / "rides"
    merge_rides(prepare_bronze(original), path)
    with pytest.raises(ValueError, match="Conflicting payload"):
        merge_rides(prepare_bronze(changed), path)
    assert spark.read.format("delta").load(str(path)).count() == 1


def test_incorrect_partition_fails_before_write(spark, tmp_path):
    good, _ = split(raw_df(spark, [row()]))
    incorrect = prepare_bronze(good).withColumn("year", F.lit(2020))
    with pytest.raises(ValueError, match="Partition columns"):
        merge_rides(incorrect, tmp_path / "invalid")


def test_city_history_merge_preserves_closed_version(spark, tmp_path):
    path = tmp_path / "city"
    before = update_city_history(
        empty_city_history(spark), spark.createDataFrame([("Mumbai", "West")], ["city_name", "region"]),
        datetime(2023, 1, 1, tzinfo=timezone.utc)).localCheckpoint(eager=True)
    merge_city_history(before, path)
    stored = spark.read.format("delta").load(str(path))
    after = update_city_history(
        stored, spark.createDataFrame([("Mumbai", "New West")], ["city_name", "region"]),
        datetime(2025, 1, 1, tzinfo=timezone.utc)).localCheckpoint(eager=True)
    merge_city_history(after, path)
    merge_city_history(after, path)
    actual = spark.read.format("delta").load(str(path))
    versions = {r["region"]: r["is_current"] for r in actual.select("region", "is_current").collect()}
    assert versions == {"West": False, "New West": True}
    assert actual.count() == 2


def test_pruning_reports_observed_files_and_sizes(spark, tmp_path):
    path = tmp_path / "bronze"
    (spark.range(0, 200, 1, 2).withColumn("year", F.lit(2024))
     .withColumn("month", F.lit(1)).withColumn("day", F.when(F.col("id") < 100, 1).otherwise(2))
     .write.format("delta").partitionBy("year", "month", "day").save(str(path)))
    report = run_pruning_demo(spark, {"paths": {"bronze": str(path)}}, "2024-01-01")
    full, selected = report["full_scan"], report["filtered_scan"]
    assert full["rows"] == 200 and selected["rows"] == 100
    assert 0 < selected["distinct_input_files_emitting_rows"] < full["distinct_input_files_emitting_rows"]
    assert 0 < selected["sum_on_disk_bytes_of_those_files"] < full["sum_on_disk_bytes_of_those_files"]
    assert "PartitionFilters" in selected["executed_plan"]
    assert selected["sum_on_disk_bytes_of_those_files"] == sum(x["on_disk_bytes"] for x in selected["files"])


def test_compaction_demo_preserves_rows(spark, tmp_path):
    config = {"paths": {"compaction_benchmarks": str(tmp_path / "benchmarks")},
              "file_sizing": {"target_bytes": 134217728}}
    report = run_compaction_demo(spark, config)
    assert report["before"]["files"] > 0
    assert report["after"]["files"] <= report["before"]["files"]
    assert spark.read.format("delta").load(report["demo_path"]).count() == 1000


def test_generator_separate_injection_labels_are_reproducible():
    args = (1000, 42, 2024, CITIES, STATUSES)
    rows, expected, labels = generate_rows(*args, include_record_labels=True)
    rows_again, expected_again, labels_again = generate_rows(*args, include_record_labels=True)
    assert (rows, expected, labels) == (rows_again, expected_again, labels_again)
    assert len(labels) == len(rows) == 1000
    assert labels[0]["csv_line_number"] == 2
    assert all(set(record) == set(SOURCE_FIELDS) for record in rows)
    injected = Counter(label["injected_defect"] for label in labels)
    assert {key: injected[key] for key in expected["injected_defects"]} == expected["injected_defects"]
    assert sum(label["simulated_late_arrival"] for label in labels) == expected["late_arriving_count"]
