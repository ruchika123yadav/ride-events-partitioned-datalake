"""Read one local ride CSV into a raw Spark DataFrame with provenance.

This module does not validate business rules, cast types, or write Delta data.
Malformed CSV records are exposed through `_corrupt_record` for validation to
quarantine. A schema/header mismatch is a batch-level error, never silently
mapped by position. Spark evaluation remains lazy until an action is executed.
"""

import csv
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


SOURCE_FIELDS = (
    "ride_id", "rider_id", "driver_id", "city", "fare", "event_time", "status"
)
CORRUPT_FIELD = "_corrupt_record"
RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SOURCE_FIELDS]
    + [StructField(CORRUPT_FIELD, StringType(), True)]
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def ingest_csv(spark, source_path, run_id):
    """Return raw rows, including source_file and run_id, from one local CSV.

    Relative paths are resolved against the project root. A zero-byte file or
    header-only file represents an empty batch; a missing file or unexpected
    header raises an error. A malformed row's original CSV text is made available
    in `_corrupt_record` by Spark's permissive parser when it flags that row.
    The validation stage must explicitly handle that column before accepting rows.
    """
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string")
    path = Path(source_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Source CSV file does not exist: {path}")

    # Check header before handing the file to Spark: prevent schema drift from
    # being silently interpreted as another column. utf-8-sig tolerates a BOM.
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle), None)
    if header is None:
        raw = spark.createDataFrame([], RAW_SCHEMA)
    else:
        if tuple(header) != SOURCE_FIELDS:
            raise ValueError(f"Unexpected CSV header: expected {SOURCE_FIELDS}, got {tuple(header)}")
        # Spark's CSV corrupt-record detection can change with column pruning.
        # Disable it for this session so downstream projections cannot hide errors.
        spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
        raw = (spark.read
               .format("csv")
               .schema(RAW_SCHEMA)
               .option("header", "true")
               .option("encoding", "UTF-8")
               .option("mode", "PERMISSIVE")
               .option("columnNameOfCorruptRecord", CORRUPT_FIELD)
               .load(str(path)))

    # Keep public artifacts free of user-specific absolute local paths. The
    # pipeline also records a content fingerprint to distinguish source batches.
    return (raw.withColumn("run_id", F.lit(run_id))
            .withColumn("source_file", F.lit(path.name)))
