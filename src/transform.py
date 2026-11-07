"""Pure Spark transformations for Bronze, Silver and Gold ride datasets.

These functions return DataFrames; they do not persist or merge Delta tables.
The caller is responsible for atomic writes, recovery, and persisting city
history before publishing facts. Timestamps and partition dates use UTC.
"""

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType


CITY_SCHEMA = StructType([
    StructField("city_key", StringType(), False),
    StructField("city_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("valid_from", TimestampType(), False),
    StructField("valid_to", TimestampType(), True),
    StructField("is_current", BooleanType(), False),
])
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX"


def _require_utc(spark):
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session timezone must be UTC")


def _instant(value):
    """Turn an aware Python datetime into an explicit UTC Spark timestamp."""
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("effective_at must be a timezone-aware datetime")
    text = value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return F.try_to_timestamp(F.lit(text), F.lit(TIMESTAMP_FORMAT))


def empty_city_history(spark):
    """A typed empty city dimension for the first run."""
    return spark.createDataFrame([], CITY_SCHEMA)


def prepare_bronze(valid_df):
    """Derive UTC event timestamp and year/month/day for validated records.

    Original event_time and other raw fields are kept alongside event_timestamp;
    the later writer must partition using the derived fields.
    """
    _require_utc(valid_df.sparkSession)
    event_timestamp = F.try_to_timestamp(F.col("event_time"), F.lit(TIMESTAMP_FORMAT))
    return (valid_df.withColumn("event_timestamp", event_timestamp)
            .withColumn("year", F.year("event_timestamp"))
            .withColumn("month", F.month("event_timestamp"))
            .withColumn("day", F.dayofmonth("event_timestamp")))


def prepare_silver(bronze_df):
    """Cast fare and event_time and retain the latest event per ride_id.

    Deterministic hash plus canonical JSON resolves equal timestamps. This
    operates on the supplied DataFrame; merging with stored state is separate.
    """
    _require_utc(bronze_df.sparkSession)
    source_columns = ["ride_id", "rider_id", "driver_id", "city", "fare", "event_time", "status"]
    canonical = F.to_json(F.struct(*[F.col(name) for name in source_columns]), {"ignoreNullFields": "false"})
    rank = Window.partitionBy("ride_id").orderBy(
        F.col("event_timestamp").desc(), F.sha2(canonical, 256), canonical
    )
    return (bronze_df.withColumn("_rank", F.row_number().over(rank))
            .filter(F.col("_rank") == 1).drop("_rank", "_corrupt_record")
            .withColumn("fare", F.col("fare").cast("double"))
            .drop("event_time")
            .withColumnRenamed("event_timestamp", "event_time"))


def update_city_history(history_df, changes_df, effective_at):
    """Return SCD2 city history after applying one city/region snapshot.

    ``changes_df`` contains city_name and region, at most once per city. For
    newly discovered cities the caller chooses the initial effective_at; to
    cover historical rides, bootstrap with an appropriately early timestamp.
    Changed regions close the current [valid_from, valid_to) interval and open
    a new one. Same-value retries produce no extra version. Retroactive or
    simultaneous changes to the current region are rejected, not rewritten.
    """
    spark = history_df.sparkSession
    _require_utc(spark)
    effective = _instant(effective_at)
    required = set(CITY_SCHEMA.fieldNames())
    if not required.issubset(history_df.columns):
        raise ValueError("city history is missing required columns")
    if not {"city_name", "region"}.issubset(changes_df.columns):
        raise ValueError("city changes require city_name and region")
    changes = changes_df.select("city_name", "region")
    if changes.filter(F.col("city_name").isNull() | (F.trim("city_name") == "") |
                      F.col("region").isNull() | (F.trim("region") == "")).limit(1).count():
        raise ValueError("city names and regions must be non-empty")
    if changes.groupBy("city_name").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("multiple changes for one city in the same snapshot")
    current = history_df.filter(F.col("is_current")).select(
        F.col("city_name").alias("_old_city"), F.col("region").alias("_old_region"),
        F.col("valid_from").alias("_old_from")
    )
    if current.groupBy("_old_city").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("city history has multiple current rows for a city")
    candidates = changes.join(current, changes.city_name == current._old_city, "left")
    changes_only = candidates.filter(F.col("_old_city").isNull() | (F.col("region") != F.col("_old_region")))
    if changes_only.filter(F.col("_old_city").isNotNull() & (effective <= F.col("_old_from"))).limit(1).count():
        raise ValueError("city region changes must be later than the current version")

    changed_names = changes_only.filter(F.col("_old_city").isNotNull()).select(
        F.col("city_name").alias("_changed_city")
    )
    closed = (history_df.join(changed_names, history_df.city_name == changed_names._changed_city, "left")
              .withColumn("valid_to", F.when(F.col("_changed_city").isNotNull() & F.col("is_current"), effective)
                          .otherwise(F.col("valid_to")))
              .withColumn("is_current", F.when(F.col("_changed_city").isNotNull() & F.col("is_current"), F.lit(False))
                          .otherwise(F.col("is_current")))
              .drop("_changed_city"))
    new_rows = (changes_only.select("city_name", "region")
                .withColumn("valid_from", effective)
                .withColumn("valid_to", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True)))
    canonical = F.to_json(F.struct("city_name", "region", "valid_from"), {"ignoreNullFields": "false"})
    new_rows = new_rows.withColumn("city_key", F.sha2(canonical, 256)).select(*CITY_SCHEMA.fieldNames())
    return closed.select(*CITY_SCHEMA.fieldNames()).unionByName(new_rows)


def prepare_gold_fact(silver_df, city_history_df):
    """Resolve each ride to the city version valid at its event_time.

    Intervals are left-closed/right-open. Missing or overlapping city versions
    raise an error rather than silently dropping a ride or duplicating a fact.
    """
    _require_utc(silver_df.sparkSession)
    rides = silver_df.alias("r")
    cities = city_history_df.alias("c")
    match = ((F.col("r.city") == F.col("c.city_name")) &
             (F.col("r.event_time") >= F.col("c.valid_from")) &
             (F.col("c.valid_to").isNull() | (F.col("r.event_time") < F.col("c.valid_to"))))
    joined = rides.join(cities, match, "left")
    if joined.filter(F.col("c.city_key").isNull()).limit(1).count():
        raise ValueError("ride has no city dimension version valid at event_time")
    if joined.groupBy(F.col("r.ride_id")).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("ride matches multiple city versions or duplicate silver IDs")
    return joined.select(F.col("r.ride_id"), F.col("r.rider_id"), F.col("r.driver_id"),
                         F.col("c.city_key"), F.col("r.fare"), F.col("r.event_time"))
