"""Create a readable summary from reports produced by one completed run."""

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path


def table(headers, rows):
    def cell(value):
        return str(value if value is not None else "—").replace("|", "\\|").replace("\n", " ")
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    lines.extend("| " + " | ".join(cell(value) for value in row) + " |" for row in rows)
    return "\n".join(lines)


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def create_summary(run_dir):
    run_dir = Path(run_dir)
    reports = run_dir / "reports"
    first, rerun = load(reports / "pipeline_first.json"), load(reports / "pipeline_rerun.json")
    idem = load(reports / "idempotency.json")
    compaction, pruning = load(reports / "compaction.json"), load(reports / "pruning.json")
    late, city = load(reports / "late_arrival.json"), load(reports / "city_history.json")
    info = load(run_dir / "run_info.json")
    xml = ET.parse(run_dir / "pytest-results.xml").getroot()
    suites = [xml] if xml.tag == "testsuite" else xml.findall("testsuite")
    if not suites:
        raise ValueError("No pytest testsuite found in pytest-results.xml")
    totals = {name: sum(int(suite.attrib.get(name, 0)) for suite in suites)
              for name in ("tests", "failures", "errors", "skipped")}
    passed = totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"]
    if totals["failures"] or totals["errors"]:
        raise ValueError("Pytest results contain failures or errors")
    if idem["status"] != "passed":
        raise ValueError("Idempotency comparison did not pass")

    counts = [("Source", "source_count"), ("Quarantine", "quarantine_count"),
              ("Bronze batch", "bronze_batch_count"), ("Silver batch", "silver_batch_count"),
              ("Gold batch", "gold_batch_count")]
    count_table = table(["Metric", "First execution", "Replay"],
                        [(name, first[key], rerun[key]) for name, key in counts])
    full, filtered = pruning["full_scan"], pruning["filtered_scan"]
    scan_rows = [(name, scan["rows"], scan["distinct_input_files_emitting_rows"],
                  scan["sum_on_disk_bytes_of_those_files"], scan["elapsed_seconds"])
                 for name, scan in (("Full", full), ("One day", filtered))]
    compact_rows = [(name, result["files"], round(result["average_file_bytes"], 2))
                    for name, result in (("Before", compaction["before"]), ("After", compaction["after"]))]
    city_rows = [(phase, row["city_name"], row["region"], row["valid_from_utc"],
                  row["valid_to_utc"], row["is_current"])
                 for phase in ("before", "after") for row in city[phase]]
    mapping_rows = [(row["ride_id"], row["region"]) for row in city["ride_version_mapping"]]
    lines = [
        f"# Reviewer summary: {info['run_folder']}",
        f"Created at UTC: {info['created_at_utc']}",
        "## Reconciliation and replay", count_table,
        f"Idempotency: **{idem['status']}**; counts and table fingerprints match. "
        "Fingerprints are non-cryptographic checks, not proof against all collisions.",
        "## Partition pruning",
        table(["Scan", "Rows", "Files emitting rows", "On-disk bytes of those files", "Seconds"], scan_rows),
        f"Day: {pruning['day']}. File reduction: {pruning['files_emitting_rows_reduction_percent']}%.",
        "On-disk file sizes are not physical bytes transferred; files emitting no rows are excluded. "
        "Inspect pruning.json for the execution plan and available Spark SQL metrics.",
        "## Compaction (isolated demo)",
        table(["Stage", "Active files", "Average file bytes"], compact_rows),
        f"File reduction: {compaction['file_reduction_percent']}%. "
        "The demo does not compact production pipeline tables.",
        "## Late-arriving event (isolated demo)",
        table(["Observation", "Result"], [
            ("First batch event (UTC)", late["first_batch_event_time"]),
            ("Later batch event (UTC)", late["late_batch_event_time"]),
            ("Observed Delta partition", late["observed_partition"]),
            ("Rows before / after", f"{late['rows_before_second_batch']} / {late['rows_after_second_batch']}"),
            ("Historical partition rows before / after", f"{late['historical_partition_rows_before']} / {late['historical_partition_rows_after']}"),
        ]),
        "The later batch adds an older event to an existing historical partition derived from event time.",
        "## City history SCD Type 2 (isolated demo)",
        table(["Snapshot", "City", "Region", "Valid from UTC", "Valid to UTC", "Current"], city_rows),
        table(["Ride", "Matched historical region"], mapping_rows),
        "Region names in this controlled example are illustrative.",
        "## Automated tests",
        table(["Tests", "Passed", "Failed", "Errors", "Skipped"],
              [(totals["tests"], passed, totals["failures"], totals["errors"], totals["skipped"])]),
        "See `../pytest-results.xml` and `../logs/08-pytest.log` for test details.",
    ]
    return "\n\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, help="Directory for one completed run")
    args = parser.parse_args()
    output = Path(args.run_dir) / "reports" / "reviewer_summary.md"
    if output.exists():
        raise FileExistsError(f"Summary already exists: {output}")
    summary = create_summary(args.run_dir)
    output.write_text(summary, encoding="utf-8")
    print(summary)
    print(f"Reviewer summary saved to: {output}")


if __name__ == "__main__":
    main()
