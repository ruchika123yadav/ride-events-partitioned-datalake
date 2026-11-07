"""Run an isolated Delta compaction experiment; never modifies pipeline tables."""

import argparse
import json
import time
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from run_pipeline import load_config, make_spark, project_path, write_json


def run_demo(spark, config):
    root = project_path(config["paths"]["compaction_benchmarks"])
    path = str(root / uuid4().hex)
    # Multiple tasks deliberately write files into the SAME daily partition.
    demo = (spark.range(0, 1000, 1, 4).repartition(4)
            .withColumn("year", F.lit(2024)).withColumn("month", F.lit(1))
            .withColumn("day", F.lit(1)))
    demo.write.format("delta").mode("errorifexists").partitionBy("year", "month", "day").save(path)
    table = DeltaTable.forPath(spark, path)

    def metrics():
        detail = DeltaTable.forPath(spark, path).detail().select("numFiles", "sizeInBytes").first()
        return {"files": detail["numFiles"], "bytes": detail["sizeInBytes"],
                "average_file_bytes": detail["sizeInBytes"] / detail["numFiles"] if detail["numFiles"] else 0}

    before = metrics()
    start = time.perf_counter()
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", config["file_sizing"]["target_bytes"])
    table.optimize().executeCompaction().collect()
    elapsed = time.perf_counter() - start
    after = metrics()
    reduction = (before["files"] - after["files"]) / before["files"] * 100 if before["files"] else 0
    return {"demo_path": path, "before": before, "after": after,
            "file_reduction_percent": round(reduction, 2), "optimize_seconds": round(elapsed, 3),
            "note": "Isolated demo; active Delta files only. Compaction cannot combine separate partitions."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.yaml"))
    args = parser.parse_args()
    config = load_config(args.config)
    spark = make_spark(config)
    try:
        result = run_demo(spark, config)
        output = project_path(config["paths"]["reconciliation"]) / ("optimize_" + uuid4().hex + ".json")
        write_json(output, result)
        print(json.dumps(result, indent=2))
        print(f"Report: {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
