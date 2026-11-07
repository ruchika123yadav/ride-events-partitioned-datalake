#!/usr/bin/env bash
# Each invocation creates a new numbered folder; existing runs are preserved.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON="$ROOT/.venv/bin/python"
else
  PYTHON="${PYTHON:-python3}"
fi
mkdir -p "$ROOT/output/runs"

# Find the highest existing run number. Timestamp-named legacy folders are left
# untouched. mkdir is atomic, so concurrent attempts cannot use the same folder.
highest=0
for existing in "$ROOT"/output/runs/run[0-9]*; do
  [[ -d "$existing" ]] || continue
  name="${existing##*/}"
  if [[ "$name" =~ ^run([0-9]+)$ ]]; then
    number=$((10#${BASH_REMATCH[1]}))
    if (( number > highest )); then highest=$number; fi
  fi
done
number=$((highest + 1))
while true; do
  printf -v RUN_NAME 'run%02d' "$number"
  RUN_DIR="$ROOT/output/runs/$RUN_NAME"
  if mkdir "$RUN_DIR" 2>/dev/null; then break; fi
  if [[ ! -e "$RUN_DIR" ]]; then
    echo "Could not create run directory: $RUN_DIR" >&2
    exit 1
  fi
  number=$((number + 1))
done
export ROOT RUN_DIR RUN_NAME PYTHON
export PYTHONUNBUFFERED=1
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
mkdir -p "$RUN_DIR/logs"
trap 'code=$?; printf "\nRun failed (exit %s). See logs in %s/logs\n" "$code" "$RUN_DIR" >&2' ERR
echo "Run output: $RUN_DIR"

"$PYTHON" - <<'PY' 2>&1 | tee "$RUN_DIR/logs/00-configuration.log"
import json, os
from datetime import datetime, timezone
from pathlib import Path
import yaml
root, run = Path(os.environ["ROOT"]), Path(os.environ["RUN_DIR"])
with (root / "config.yaml").open(encoding="utf-8") as handle:
    config = yaml.safe_load(handle)
base = run / "lake"
config["paths"].update({
    "source": str(run / "input" / "rides.csv"),
    "expected": str(run / "expected"),
    "bronze": str(base / "bronze"),
    "silver": str(base / "silver"),
    "gold_facts": str(base / "gold" / "ride_fact"),
    "gold_cities": str(base / "gold" / "city_dim"),
    "quarantine": str(base / "quarantine"),
    "run_logs": str(run / "run_logs"),
    "reconciliation": str(run / "reports"),
    "compaction_benchmarks": str(run / "demonstrations" / "compaction"),
    "reviewer_demo": str(run / "demonstrations" / "late_and_city"),
})
created = datetime.now(timezone.utc).isoformat()
# Freeze a single cutoff so the first run and replay evaluate the same rules.
config["validation"]["as_of_utc"] = created
with (run / "config.yaml").open("w", encoding="utf-8") as handle:
    yaml.safe_dump(config, handle, sort_keys=False)
with (run / "run_info.json").open("w", encoding="utf-8") as handle:
    json.dump({"run_folder": os.environ["RUN_NAME"], "created_at_utc": created,
               "description": "One full attempt containing a first pipeline execution and an idempotency replay."}, handle, indent=2)
    handle.write("\n")
print("Created isolated run configuration and run_info.json.")
PY

CONFIG="$RUN_DIR/config.yaml"
echo '[1/7] Generate reproducible source and separate labels'
"$PYTHON" src/generate_data.py --config "$CONFIG" --labels-output "$RUN_DIR/expected/record_labels.csv" 2>&1 | tee "$RUN_DIR/logs/01-generation.log"
echo '[2/7] Run pipeline for the first time'
"$PYTHON" src/run_pipeline.py --config "$CONFIG" --label first 2>&1 | tee "$RUN_DIR/logs/02-pipeline-first.log"
echo '[3/7] Rerun pipeline and compare results'
"$PYTHON" src/run_pipeline.py --config "$CONFIG" --label rerun 2>&1 | tee "$RUN_DIR/logs/03-pipeline-rerun.log"
"$PYTHON" - <<'PY' 2>&1 | tee "$RUN_DIR/logs/04-idempotency.log"
import json, os
from pathlib import Path
run = Path(os.environ["RUN_DIR"])
reports = [run / "reports" / "pipeline_first.json", run / "reports" / "pipeline_rerun.json"]
values = [json.loads(path.read_text(encoding="utf-8")) for path in reports]
expected = json.loads((run / "expected" / "rides_expected.json").read_text(encoding="utf-8"))
fields = ("source_count", "quarantine_count", "bronze_batch_count", "silver_batch_count",
          "gold_batch_count", "bronze_total_count", "silver_total_count", "gold_total_count",
          "bronze_fingerprint", "silver_fingerprint", "gold_fingerprint")
for field in fields:
    if field not in values[0] or field not in values[1] or values[0][field] != values[1][field]:
        raise SystemExit(f"Rerun mismatch or missing field: {field}")
if (values[0]["source_count"] != expected["source_count"] or
        values[0]["quarantine_count"] != expected["expected_rejected_count"] or
        values[0]["bronze_batch_count"] != expected["expected_valid_count"]):
    raise SystemExit("Pipeline counts differ from generator expectations")
report = {"status": "passed", "comparison": "counts and non-cryptographic table fingerprints",
          "first": {field: values[0][field] for field in fields},
          "rerun": {field: values[1][field] for field in fields},
          "note": "Fingerprint equality is useful evidence, not a proof against all collisions."}
(run / "reports" / "idempotency.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print("IDEMPOTENCY: counts and table fingerprints match.")
print(f"{'Layer':<14} {'First run':>12} {'Rerun':>12}")
for title, field in [("Source", "source_count"), ("Quarantine", "quarantine_count"),
                     ("Bronze", "bronze_total_count"), ("Silver", "silver_total_count"),
                     ("Gold", "gold_total_count")]:
    print(f"{title:<14} {values[0][field]:>12} {values[1][field]:>12}")
PY
echo '[4/7] Demonstrate isolated compaction'
"$PYTHON" src/optimize.py --config "$CONFIG" --report-name compaction.json 2>&1 | tee "$RUN_DIR/logs/05-compaction.log"
echo '[5/7] Demonstrate partition pruning'
"$PYTHON" src/prune_demo.py --config "$CONFIG" --day auto --report-name pruning.json 2>&1 | tee "$RUN_DIR/logs/06-pruning.log"
echo '[6/7] Demonstrate late arrival and city history'
"$PYTHON" src/reviewer_demo.py --config "$CONFIG" 2>&1 | tee "$RUN_DIR/logs/07-reviewer-demo.log"
echo '[7/7] Run automated tests'
"$PYTHON" -m pytest -q --junitxml="$RUN_DIR/pytest-results.xml" 2>&1 | tee "$RUN_DIR/logs/08-pytest.log"
echo 'Creating reviewer summary'
"$PYTHON" src/reviewer_summary.py --run-dir "$RUN_DIR" 2>&1 | tee "$RUN_DIR/logs/09-reviewer-summary.log"
trap - ERR
echo "Completed. Review $RUN_DIR/reports/reviewer_summary.md"
