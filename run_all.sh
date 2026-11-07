#!/usr/bin/env bash
# Fresh isolated run: never deletes or overwrites a previous run's data.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON="$ROOT/.venv/bin/python"
else
  PYTHON="${PYTHON:-python3}"
fi
mkdir -p "$ROOT/output/runs"
RUN_DIR="$(mktemp -d "$ROOT/output/runs/$(date -u +%Y%m%dT%H%M%SZ)_XXXXXX")"
export ROOT RUN_DIR PYTHON
export PYTHONUNBUFFERED=1
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
mkdir -p "$RUN_DIR/logs"
trap 'code=$?; printf "\nRun failed (exit %s). See logs in %s/logs\n" "$code" "$RUN_DIR" >&2' ERR
echo "Run output: $RUN_DIR"

# Keep every data artifact and the effective configuration under this run.
"$PYTHON" - <<'PY' 2>&1 | tee "$RUN_DIR/logs/00-config.log"
import os
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
    "reconciliation": str(run / "reconciliation"),
    "compaction_benchmarks": str(run / "benchmarks" / "compaction"),
})
# Freeze one real cutoff for both pipeline invocations in this run.
config["validation"]["as_of_utc"] = datetime.now(timezone.utc).isoformat()
with (run / "config.yaml").open("w", encoding="utf-8") as handle:
    yaml.safe_dump(config, handle, sort_keys=False)
print("Created isolated run configuration.")
PY

CONFIG="$RUN_DIR/config.yaml"
echo '[1/6] Generate synthetic data'
"$PYTHON" src/generate_data.py --config "$CONFIG" 2>&1 | tee "$RUN_DIR/logs/01-generate.log"
echo '[2/6] Run pipeline'
"$PYTHON" src/run_pipeline.py --config "$CONFIG" 2>&1 | tee "$RUN_DIR/logs/02-pipeline.log"
echo '[3/6] Rerun pipeline and compare counts'
"$PYTHON" src/run_pipeline.py --config "$CONFIG" 2>&1 | tee "$RUN_DIR/logs/03-rerun.log"
"$PYTHON" - <<'PY' 2>&1 | tee "$RUN_DIR/logs/03-idempotency-check.log"
import json, os
from pathlib import Path
reports = sorted((Path(os.environ["RUN_DIR"]) / "reconciliation").glob("RUN_*.json"))
if len(reports) != 2:
    raise SystemExit(f"Expected two pipeline reconciliation reports; found {len(reports)}")
values = [json.loads(path.read_text(encoding="utf-8")) for path in reports]
expected = json.loads((Path(os.environ["RUN_DIR"]) / "expected" / "rides_expected.json").read_text(encoding="utf-8"))
fields = ("source_count", "quarantine_count", "bronze_batch_count", "silver_batch_count",
          "gold_batch_count", "bronze_total_count", "silver_total_count", "gold_total_count",
          "bronze_fingerprint", "silver_fingerprint", "gold_fingerprint")
for field in fields:
    if field not in values[0] or field not in values[1] or values[0][field] != values[1][field]:
        raise SystemExit(f"Rerun count mismatch or missing field: {field}")
if (values[0]["source_count"] != expected["source_count"] or
        values[0]["quarantine_count"] != expected["expected_rejected_count"] or
        values[0]["bronze_batch_count"] != expected["expected_valid_count"]):
    raise SystemExit("Pipeline counts differ from the generator's expected counts")
print("Rerun count and table fingerprint comparison passed:", {field: values[1][field] for field in fields})
PY
echo '[4/6] Run isolated compaction demo'
"$PYTHON" src/optimize.py --config "$CONFIG" 2>&1 | tee "$RUN_DIR/logs/04-optimize.log"
echo '[5/6] Run pruning demo on an existing day'
"$PYTHON" src/prune_demo.py --config "$CONFIG" --day auto 2>&1 | tee "$RUN_DIR/logs/05-pruning.log"
echo '[6/6] Run automated tests'
"$PYTHON" -m pytest -q --basetemp="$RUN_DIR/pytest-tmp" --junitxml="$RUN_DIR/pytest-results.xml" 2>&1 | tee "$RUN_DIR/logs/06-pytest.log"
trap - ERR
echo "Completed. Review reports and logs in $RUN_DIR"
