"""Run full/day scans and report executed Spark metrics plus observed file sizes."""

import argparse
import json
import time
from datetime import date
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
from run_pipeline import load_config, make_spark, project_path, write_json


def executed_metrics(plan):
    """Collect available SQL metrics after the action; keep raw labels/values.

    Spark's available metrics and operator names vary by version/plan. Absence
    is reported honestly rather than replaced with a fabricated measurement.
    """
    entries = []

    def visit(node):
        try:
            if node.nodeName() == "AdaptiveSparkPlan":
                visit(node.executedPlan())
                return
            it = node.metrics().iterator()
            while it.hasNext():
                item = it.next()
                metric = item._2()
                option = metric.name()
                label = option.get() if option.isDefined() else item._1()
                entries.append({"operator": node.nodeName(), "key": item._1(),
                                "label": str(label), "value": int(metric.value())})
            children = node.children().iterator()
            while children.hasNext():
                visit(children.next())
        except Exception as exc:
            entries.append({"operator": "unknown", "metric_collection_error": str(exc)})

    visit(plan)
    return entries


def measure(df):
    # input_file_name() identifies files which produce at least one output row.
    query = df.select(F.input_file_name().alias("path")).groupBy("path").count()
    start = time.perf_counter()
    per_file = query.collect()
    elapsed = time.perf_counter() - start
    plan = query._jdf.queryExecution().executedPlan()
    metrics = executed_metrics(plan)
    hadoop_conf = df.sparkSession.sparkContext._jsc.hadoopConfiguration()
    jvm = df.sparkSession._jvm
    file_sizes = []
    for row in per_file:
        p = jvm.org.apache.hadoop.fs.Path(row["path"])
        length = p.getFileSystem(hadoop_conf).getFileStatus(p).getLen()
        file_sizes.append({"path": row["path"], "rows": row["count"], "on_disk_bytes": int(length)})
    return {
        "rows": sum(item["rows"] for item in file_sizes),
        "distinct_input_files_emitting_rows": len(file_sizes),
        "sum_on_disk_bytes_of_those_files": sum(item["on_disk_bytes"] for item in file_sizes),
        "elapsed_seconds": round(elapsed, 3),
        "files": file_sizes,
        "executed_sql_metrics": metrics,
        "executed_plan": plan.toString(),
    }


def run_demo(spark, config, day):
    table = spark.read.format("delta").load(str(project_path(config["paths"]["bronze"])))
    if day == "auto":
        first = table.select("year", "month", "day").distinct().orderBy("year", "month", "day").first()
        if first is None:
            raise ValueError("Bronze is empty; no day is available for the pruning demo")
        day = date(first["year"], first["month"], first["day"])
    else:
        day = date.fromisoformat(day)
    filtered = table.filter((F.col("year") == day.year) & (F.col("month") == day.month) & (F.col("day") == day.day))
    full, selected = measure(table), measure(filtered)
    def reduction(key):
        return round(100 * (full[key] - selected[key]) / full[key], 2) if full[key] else None
    return {
        "day": day.isoformat(), "full_scan": full, "filtered_scan": selected,
        "files_emitting_rows_reduction_percent": reduction("distinct_input_files_emitting_rows"),
        "on_disk_bytes_of_files_emitting_rows_reduction_percent": reduction("sum_on_disk_bytes_of_those_files"),
        "measurement_note": (
            "File sizes are actual filesystem lengths of distinct input_file_name() files emitting rows, "
            "not physical bytes transferred. Empty files and files emitting no rows are excluded. "
            "Executed SQL metrics are included as reported by this Spark plan; inspect PartitionFilters "
            "and available file-count/file-size metrics. Metrics may be absent or differ by Spark version."
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="auto", help="UTC partition date YYYY-MM-DD, or auto")
    parser.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.yaml"))
    args = parser.parse_args()
    config = load_config(args.config)
    spark = make_spark(config)
    try:
        result = run_demo(spark, config, args.day)
        output = project_path(config["paths"]["reconciliation"]) / ("pruning_" + uuid4().hex + ".json")
        write_json(output, result)
        print(json.dumps(result, indent=2))
        print(f"Report: {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
