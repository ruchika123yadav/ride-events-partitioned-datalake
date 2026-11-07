"""Idempotent local Delta writers for ride data and city history.

Spark must already be configured with Delta extensions and a UTC session.
Writes are atomic per Delta table, not across Bronze, Silver, Gold and logs.
The caller owns retries, locking/concurrency policy and pipeline reconciliation.
"""

from functools import reduce
from operator import and_
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARTITIONS = ("year", "month", "day")
CITY_COLUMNS = ("city_key", "city_name", "region", "valid_from", "valid_to", "is_current")


def _path(value):
    path = Path(value)
    return str((path if path.is_absolute() else PROJECT_ROOT / path).resolve())


def _check_unique(df, key):
    if key not in df.columns:
        raise ValueError(f"Missing key column: {key}")
    if df.filter(F.col(key).isNull() | (F.trim(F.col(key)) == "")).limit(1).count():
        raise ValueError(f"Null or blank key: {key}")
    if df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Duplicate key in source: {key}")


def _existing_table(df, path, partitions):
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        if Path(path).exists():
            raise ValueError(f"Path already exists and is not a Delta table: {path}")
        return None
    table = DeltaTable.forPath(spark, path)
    target_schema = {f.name: f.dataType for f in table.toDF().schema.fields}
    source_schema = {f.name: f.dataType for f in df.schema.fields}
    if source_schema != target_schema:
        raise ValueError("Source schema does not match existing Delta table")
    actual_partitions = tuple(table.detail().select("partitionColumns").first()[0])
    if actual_partitions != tuple(partitions):
        raise ValueError(f"Existing partition columns {actual_partitions} do not match {tuple(partitions)}")
    return table


def _equal_columns(columns):
    return reduce(and_, (F.col(f"t.`{name}`").eqNullSafe(F.col(f"s.`{name}`")) for name in columns))


def merge_rides(df, destination, timestamp_column="event_timestamp", partitioned=True):
    """Insert immutable ride rows by ride_id; exact replays do not add rows.

    A ride_id with a different payload is a conflict and fails explicitly.
    run_id/source_file are excluded from the comparison, preserving first-seen
    provenance on replay. This honors the current one-record-per-ride contract;
    updates/multiple ride events need a separate event key and history design.
    Returns False for an empty batch and True when a write or merge was attempted.
    """
    spark = df.sparkSession
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session timezone must be UTC")
    _check_unique(df, "ride_id")
    if not df.limit(1).count():
        return False
    if timestamp_column not in df.columns:
        raise ValueError(f"Missing timestamp column: {timestamp_column}")
    ts = F.col(timestamp_column)
    if df.filter(ts.isNull()).limit(1).count():
        raise ValueError("Event timestamp cannot be null")
    partitions = PARTITIONS if partitioned else ()
    if partitioned:
        if not set(PARTITIONS).issubset(df.columns):
            raise ValueError("Daily partitions require year, month and day")
        mismatch = (~F.col("year").eqNullSafe(F.year(ts)) |
                    ~F.col("month").eqNullSafe(F.month(ts)) |
                    ~F.col("day").eqNullSafe(F.dayofmonth(ts)))
        if df.filter(mismatch).limit(1).count():
            raise ValueError("Partition columns must match UTC event timestamp")
    path = _path(destination)
    table = _existing_table(df, path, partitions)
    if table is None:
        writer = df.write.format("delta").mode("errorifexists")
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.save(path)
        return True

    payload = [name for name in df.columns if name not in {"run_id", "source_file", "_corrupt_record"}]
    matched = table.toDF().alias("t").join(df.alias("s"), F.col("t.ride_id") == F.col("s.ride_id"))
    if matched.filter(~_equal_columns(payload)).limit(1).count():
        raise ValueError("Conflicting payload for existing ride_id; no rows were written")
    (table.alias("t").merge(df.alias("s"), "t.ride_id = s.ride_id")
     .whenNotMatchedInsertAll().execute())
    return True


def merge_city_history(snapshot_df, destination):
    """Merge a complete SCD2 history snapshot by city_key, without deleting history.

    Existing closed versions are immutable. A current version may only be closed
    with a valid end time; all identity and effective-start fields are immutable.
    Input must include every existing version and exactly one current version per
    city. Concurrent snapshots require external coordination/Delta retry logic.
    """
    if tuple(snapshot_df.columns) != CITY_COLUMNS:
        raise ValueError(f"City snapshot columns must be {CITY_COLUMNS}")
    _check_unique(snapshot_df, "city_key")
    if not snapshot_df.limit(1).count():
        return False
    invalid = (F.col("city_name").isNull() | (F.trim("city_name") == "") |
               F.col("region").isNull() | (F.trim("region") == "") |
               F.col("valid_from").isNull() | F.col("is_current").isNull() |
               (F.col("is_current") & F.col("valid_to").isNotNull()) |
               (~F.col("is_current") & F.col("valid_to").isNull()) |
               (F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))))
    if snapshot_df.filter(invalid).limit(1).count():
        raise ValueError("Invalid city version interval or required field")
    current_counts = snapshot_df.groupBy("city_name").agg(F.sum(F.col("is_current").cast("int")).alias("n"))
    if current_counts.filter(F.col("n") != 1).limit(1).count():
        raise ValueError("Each city must have exactly one current version")
    order = Window.partitionBy("city_name").orderBy("valid_from", "city_key")
    intervals = snapshot_df.withColumn("_next_from", F.lead("valid_from").over(order))
    overlap = F.col("_next_from").isNotNull() & (F.col("valid_to").isNull() | (F.col("valid_to") > F.col("_next_from")))
    if intervals.filter(overlap).limit(1).count():
        raise ValueError("City validity intervals overlap")
    path = _path(destination)
    table = _existing_table(snapshot_df, path, ())
    if table is None:
        snapshot_df.write.format("delta").mode("errorifexists").save(path)
        return True
    existing = table.toDF()
    if existing.join(snapshot_df.select("city_key"), "city_key", "left_anti").limit(1).count():
        raise ValueError("City snapshot must preserve every existing history version")
    joined = existing.alias("t").join(snapshot_df.alias("s"), F.col("t.city_key") == F.col("s.city_key"))
    immutable = _equal_columns(("city_key", "city_name", "region", "valid_from"))
    same_state = _equal_columns(("valid_to", "is_current"))
    allowed_close = (F.col("t.is_current") & ~F.col("s.is_current") &
                     F.col("t.valid_to").isNull() & F.col("s.valid_to").isNotNull())
    if joined.filter(~immutable | ~(same_state | allowed_close)).limit(1).count():
        raise ValueError("City snapshot modifies immutable or already closed history")
    (table.alias("t").merge(snapshot_df.alias("s"), "t.city_key = s.city_key")
     .whenMatchedUpdateAll(condition="t.is_current = true AND s.is_current = false")
     .whenNotMatchedInsertAll().execute())
    return True
