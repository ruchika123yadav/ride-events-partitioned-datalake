"""Create reproducible synthetic ride data and separate expected counts.

Paths from config.yaml are relative to the project root. No Spark session is
needed for generation. Rows intentionally contain a few data-quality defects.
"""

import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIELDS = ("ride_id", "rider_id", "driver_id", "city", "fare", "event_time", "status")


def project_path(value):
    """Resolve configured relative paths consistently, regardless of cwd."""
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def generate_rows(size, seed, year, cities, statuses):
    """Return rows and generator-known labels; no labels go into the CSV.

    Defect categories are disjoint, so their counts are easy to reconcile.
    Duplicate IDs refer to otherwise clean records. Late rows are placed at the
    end of the file with earlier event timestamps, simulating arrival order.
    """
    if size < 0:
        raise ValueError("size must be non-negative")
    if not cities or not statuses:
        raise ValueError("cities and statuses must not be empty")
    rng = random.Random(seed)
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    seconds_in_year = int((end - start).total_seconds())

    rows = []
    for i in range(size):
        # Spread events throughout the year, then randomize within each interval.
        lower = i * seconds_in_year // size
        upper = max(lower, (i + 1) * seconds_in_year // size - 1)
        event_time = start + timedelta(seconds=rng.randint(lower, upper))
        rows.append({
            "ride_id": f"RIDE_{i + 1:08d}",
            "rider_id": f"RIDER_{rng.randint(1, max(1, size // 5)):08d}",
            "driver_id": f"DRIVER_{rng.randint(1, max(1, size // 10)):08d}",
            "city": rng.choice(cities),
            "fare": f"{rng.randint(5000, 300000) / 100:.2f}",
            "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": rng.choice(statuses),
        })

    indexes = list(range(size))
    rng.shuffle(indexes)
    counts = {
        "missing_city": int(size * 0.025),
        "missing_event_time": int(size * 0.025),
        "duplicate_ride_id": int(size * 0.03),
        "invalid_fare": int(size * 0.02),
        "invalid_city": int(size * 0.005),
    }
    groups = {}
    cursor = 0
    for label, count in counts.items():
        groups[label] = indexes[cursor:cursor + count]
        cursor += count
    clean = indexes[cursor:]

    for i in groups["missing_city"]:
        rows[i]["city"] = ""
    for i in groups["missing_event_time"]:
        rows[i]["event_time"] = ""
    for i in groups["invalid_fare"]:
        rows[i]["fare"] = rng.choice(["0.00", "-25.00"])
    for i in groups["invalid_city"]:
        rows[i]["city"] = "Invalid City"
    for i, original in zip(groups["duplicate_ride_id"], clean):
        rows[i]["ride_id"] = rows[original]["ride_id"]

    # Late events are valid, not rejects. Their arrival order is the CSV row order.
    late_indexes = set(clean[:int(size * 0.01)])
    for i in late_indexes:
        late_time = end - timedelta(days=rng.randint(2, 5), seconds=rng.randint(0, 86399))
        rows[i]["event_time"] = late_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    normal = [row for i, row in enumerate(rows) if i not in late_indexes]
    normal.sort(key=lambda row: row["event_time"])
    late = [row for i, row in enumerate(rows) if i in late_indexes]
    rng.shuffle(late)
    rejected = sum(counts.values())
    labels = {
        "seed": seed,
        "year": year,
        "source_count": size,
        "expected_valid_count": size - rejected,
        "expected_rejected_count": rejected,
        "injected_defects": counts,
        "late_arriving_count": len(late),
        "notes": "Counts describe generator-injected defects, not pipeline verification. CSV row order simulates arrival order.",
    }
    return normal + late, labels


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config.yaml"))
    parser.add_argument("--size", type=int, help="Number of rows (default: config.yaml)")
    parser.add_argument("--output", help="CSV output path; relative to project root")
    parser.add_argument("--expected-output", help="Expected-count JSON path; relative to project root")
    parser.add_argument("--force", action="store_true", help="Replace existing output files")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    settings = config["generator"]
    size = settings["size"] if args.size is None else args.size
    output = project_path(args.output or config["paths"]["source"])
    expected = project_path(args.expected_output or str(Path(config["paths"]["expected"]) / "rides_expected.json"))
    if output.resolve() == expected.resolve():
        parser.error("CSV and expected-count paths must be different")
    if not args.force and (output.exists() or expected.exists()):
        parser.error("Output already exists; choose new paths or pass --force")
    try:
        rows, labels = generate_rows(size, settings["seed"], settings["year"],
                                     config["validation"]["allowed_cities"],
                                     config["validation"]["allowed_statuses"])
    except ValueError as exc:
        parser.error(str(exc))
    output.parent.mkdir(parents=True, exist_ok=True)
    expected.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    with expected.open("w", encoding="utf-8") as handle:
        json.dump(labels, handle, indent=2)
        handle.write("\n")
    print(f"Generated {size:,} rows: {output}")
    print(f"Expected generator counts: {expected}")
    print(f"Valid: {labels['expected_valid_count']:,}; rejected: {labels['expected_rejected_count']:,}; late: {labels['late_arriving_count']:,}")


if __name__ == "__main__":
    main()
