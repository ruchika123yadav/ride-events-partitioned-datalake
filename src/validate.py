"""Validate ingested ride records and split valid rows from quarantine rows.

This module returns DataFrames; the pipeline orchestrator will persist the reject
DataFrame. Validation preserves the input fields, run_id, source_file and any
Spark CSV parser's _corrupt_record. No Delta write happens here.
"""

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F


SOURCE_FIELDS = (
    "ride_id", "rider_id", "driver_id", "city", "fare", "event_time", "status"
)
REQUIRED_COLUMNS = set(SOURCE_FIELDS) | {"_corrupt_record", "run_id", "source_file"}
ISO_TIMESTAMP = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$"
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX"


def validate_records(raw_df, allowed_cities, allowed_statuses, as_of):
    """Return (valid, rejects), keeping one deterministic valid row per ride_id.

    ``as_of`` must be a timezone-aware datetime and should be stored with the
    run and reused on retry. Duplicate checks here are *within this batch*;
    cross-run idempotency belongs to the later Delta writer/orchestrator.

    Business-invalid rows are quarantined before duplicate detection, so a bad
    row cannot prevent a valid row with the same ID from being accepted. For
    multiple valid candidates, the lowest canonical SHA-256 hash wins. Identical
    rows are interchangeable. This is deterministic but is not a latest-event
    policy; the source contract currently specifies one row per ride.
    """
    missing = REQUIRED_COLUMNS - set(raw_df.columns)
    if missing:
        raise ValueError(f"Missing ingestion columns: {sorted(missing)}")
    if not allowed_cities or not allowed_statuses:
        raise ValueError("allowed_cities and allowed_statuses must be non-empty")
    if not isinstance(as_of, datetime) or as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be a timezone-aware datetime")

    def blank(name):
        return F.col(name).isNull() | (F.length(F.trim(F.col(name))) == 0)

    # Tolerant casts make malformed values rejectable even with Spark ANSI mode.
    parsed_time = F.try_to_timestamp(F.col("event_time"), F.lit(TIMESTAMP_FORMAT))
    parsed_fare = F.expr("try_cast(fare AS DOUBLE)")
    cutoff = as_of.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    cutoff_timestamp = F.try_to_timestamp(F.lit(cutoff), F.lit(TIMESTAMP_FORMAT))

    # First failing rule wins; raw input is kept for investigation.
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_record")
        .when(blank("ride_id"), "missing_ride_id")
        .when(blank("event_time"), "missing_event_time")
        .when(~F.col("event_time").rlike(ISO_TIMESTAMP) | parsed_time.isNull(), "invalid_event_time")
        .when(parsed_time > cutoff_timestamp, "future_timestamp")
        .when(blank("rider_id"), "missing_rider_id")
        .when(blank("driver_id"), "missing_driver_id")
        .when(blank("city") | ~F.col("city").isin(list(allowed_cities)), "invalid_city")
        .when(blank("fare") | parsed_fare.isNull() | F.isnan(parsed_fare)
              | (F.abs(parsed_fare) == F.lit(float("inf"))) | (parsed_fare <= 0), "invalid_fare")
        .when(blank("status") | ~F.col("status").isin(list(allowed_statuses)), "invalid_status")
    )
    checked = raw_df.withColumn("rejection_reason", reason)
    rejected_invalid = checked.filter(F.col("rejection_reason").isNotNull())
    candidates = checked.filter(F.col("rejection_reason").isNull())

    # JSON of an explicitly ordered struct includes nulls, avoiding concatenation
    # ambiguity. SHA-256 ordering is stable across partitioning and input order.
    canonical = F.to_json(F.struct(*[F.col(name) for name in SOURCE_FIELDS]), {"ignoreNullFields": "false"})
    window = Window.partitionBy("ride_id").orderBy(F.sha2(canonical, 256), canonical)
    ranked = candidates.withColumn("_duplicate_rank", F.row_number().over(window))
    valid = ranked.filter(F.col("_duplicate_rank") == 1).drop("_duplicate_rank", "rejection_reason")
    rejected_duplicates = (ranked.filter(F.col("_duplicate_rank") > 1)
                           .drop("_duplicate_rank", "rejection_reason")
                           .withColumn("rejection_reason", F.lit("duplicate_ride_id")))
    rejects = rejected_invalid.unionByName(rejected_duplicates)
    return valid, rejects
