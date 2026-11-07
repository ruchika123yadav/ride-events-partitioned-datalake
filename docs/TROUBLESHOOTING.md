# Troubleshooting

All commands here are intended for the personal laptop, WSL2, or Codespace—not
the company laptop. `run_all.sh` stops on failure and reports the run directory;
inspect that directory's `logs/`, pipeline `run_logs/`, and generated config.
Treat logs as potentially sensitive before sharing publicly.

## Codespace setup or imports fail

- Wait for the container's post-create install to finish. Check the VS Code
  creation log and rebuild the container if its image/feature installation failed.
- Verify `python --version` and `java -version`; use the `.venv` interpreter.
  If needed, in the Codespace terminal run `.venv/bin/python -m pip install -r requirements.txt`.
- The pinned Delta 3.3.3 and PySpark 3.5.9 are both in the documented compatible
  3.3.x/3.5.x series. Do not independently upgrade only one dependency.
- Container builds, pip, and Delta's first JVM dependency resolution require
  network access. Maven/proxy/certificate errors must be fixed through the
  environment's trusted network/certificate configuration; do not disable TLS
  verification. After dependencies are cached, offline use may be possible.

## Java gateway, hostname, memory or slow runs

- Verify Java 17 is on `PATH` and `JAVA_HOME` points to the same JDK. The
  devcontainer Java feature normally sets it. Native Windows Spark requires
  compatible Hadoop Windows binaries; use Linux/macOS/WSL2/Codespaces instead.
- `UnknownHostException` or Java gateway errors may indicate hostname/loopback
  configuration. The runner sets `SPARK_LOCAL_IP=127.0.0.1` unless already set.
- Daily partitions with sparse data and several Delta MERGEs can be slow and
  memory-intensive. Use the default 1,000 rows first, check Codespace disk/RAM
  and GitHub quota, and do not assume a larger run will fit. Avoid reducing data
  or changing settings silently when producing final evidence.

## Source, data quality, or reconciliation errors

- `Unexpected CSV header`: provide the exact source columns and order shown in
  README. Malformed individual rows should go to quarantine; a schema mismatch
  is a batch failure.
- `future_timestamp`: compare event time to the fixed cutoff in the run's
  generated `config.yaml`. `run_all.sh` captures current UTC once for both runs.
- A conflict for an existing `ride_id` means the incoming payload is different;
  the current contract intentionally refuses overwrites. Review the source and
  use an event-history design if changed events are required.
- Missing city dimension version means its configured region/effective date did
  not cover the ride's event time. Check `city_dimension` before rerunning.
- Inspect reconciliation JSON and reject records when counts or fare totals do
  not match. Generator labels are expectations, not independent proof.

## Delta paths and retries

- A failed first write may leave a non-Delta directory. The writer refuses to
  overwrite it. Inspect it and the run log; do not delete unfamiliar data.
  A fresh `bash run_all.sh` uses a new isolated run directory.
- Partial multi-table writes are possible: there is no cross-table transaction.
  To debug a single run, rerun the same `src/run_pipeline.py --config <run config>`
  with the same source and cutoff. `run_all.sh` itself starts a fresh run.
- This example does not coordinate concurrent writers or backdated SCD updates.

## Demos or tests fail

- `Bronze is empty`: inspect the earlier pipeline and quarantine results. For a
  manually selected pruning date, choose an existing date or use `--day auto`.
- SQL metric names differ between Spark plans; inspect the recorded executed plan
  and raw metrics. Actual `input_file_name()` file sizes are reported only for
  files that emit rows, and are not physical I/O byte counts.
- The OPTIMIZE demo is isolated and may show no reduction if there are not enough
  eligible files. It must not be reported as a successful reduction unless the
  recorded before/after numbers show one.
- If pytest fails, save its terminal output and `pytest-results.xml`, including
  the **first** traceback. Do not claim tests passed; fix or disclose limitations.
