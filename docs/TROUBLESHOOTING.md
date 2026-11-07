# Troubleshooting

`run_all.sh` stops on the first failure and prints the run directory. Inspect its
`logs/`, `run_logs/`, `reports/`, and generated `config.yaml` for details.

| Symptom | Action |
| --- | --- |
| Codespace setup or import failure | Wait for post-create setup; rebuild the container or run `.venv/bin/python -m pip install -r requirements.txt`. |
| Java gateway failure | Verify Java 17 and `JAVA_HOME`; check the first Spark error. |
| Maven or certificate error | Check network, proxy, and trusted certificates; do not disable TLS verification. |
| Native Windows Hadoop error | Use Linux, macOS, WSL2, or Codespaces with a compatible Spark environment. |
| Unexpected CSV header | Match the source schema and column order in the README. |
| Future timestamp | Check the fixed validation cutoff in the run's generated configuration. |
| Conflicting ride ID | Review the source; existing ride payloads are immutable. |
| Missing city version | Check city mappings and ensure the effective interval covers the event time. |
| Reconciliation failure | Inspect quarantine and compare source, target, and fare totals. |
| Delta path is not a table | Inspect the failed write and path before any manual recovery; never delete unknown data. |
| Empty Bronze or pruning output | Inspect earlier run logs and quarantine; use `--day auto` for an existing date. |
| Demo or pytest failure | Review the first traceback, report JSON, and `pytest-results.xml`. |

Each `bash run_all.sh` starts an isolated run. To retry an existing run, use
`python src/run_pipeline.py --config <run-config>` with the same input and cutoff.
Delta commits are per table; a failure can leave a partially completed run.
Numbered folders do not rename or delete earlier timestamp-named runs. Files in
`output/runs/` are ignored by normal Git add operations but remain on disk.
