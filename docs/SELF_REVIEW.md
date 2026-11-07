# Static self-review (not execution evidence)

Reviewed from source without running the new pipeline, tests, shell script or
container on the authoring machine. This review does not prove runtime success.

| Component | Review focus and remaining qualification |
| --- | --- |
| `generate_data.py` | Seeded generator, distinct defect categories and expected labels separate from input; existing files require explicit `--force`. CSV order only simulates arrival. |
| `ingest.py` | Explicit all-string schema, exact header check, malformed field and source/run provenance; Spark CSV parser does not detect every possible malformed input. |
| `validate.py` | Tolerant parsing, finite positive fares, city/status checks, deterministic per-batch duplicate handling, explicit timezone-aware cutoff; no cross-batch check here. |
| `transform.py` | UTC partition derivation, Silver types, SCD2 intervals and as-of fact join; correct bootstrap coverage required; backdated dimension corrections unsupported. |
| `partition_writer.py` | Delta version/schema/partition checks and immutable ID MERGE policy; no concurrent-writer coordination or cross-table transaction, and native Windows Delta writes were blocked by missing Hadoop tools. |
| `run_pipeline.py` | Delta extensions/catalog, configured Spark master, quarantine, table writes, batch count/fare reconciliation and logs; input/output and generated report paths are configurable. End-to-end and Delta retry behavior are unverified. |
| `optimize.py` | Isolated single-partition fragmented data and Delta OPTIMIZE before/after metrics; does not compact production tables and reduction must be observed, not assumed. |
| `prune_demo.py` | Actual input-file paths and filesystem sizes, raw executed-plan metrics and plan; files without output rows and physical I/O bytes are not represented by the path-based count. |
| `tests/test_pipeline.py` | Tests cover core cases, SCD2, write retry, conflict, compaction and pruning; none of the newly added tests have been run in this stage. |
| `run_all.sh` | Fresh output directory, stage logs, frozen cutoff and two-run count/fingerprint check; unexecuted Bash and Codespace integration may still reveal errors. |

The pinned Python package metadata specifies `delta-spark==3.3.3` requires
`pyspark>=3.5.3,<3.6.0`; `pyspark==3.5.9` satisfies this range. The Delta
release table lists Delta 3.3.x as compatible with Spark 3.5.x. Maven artifact
availability and the full environment still need a real Codespaces run.

References: https://pypi.org/pypi/delta-spark/3.3.3/json and
https://docs.delta.io/releases/ .
