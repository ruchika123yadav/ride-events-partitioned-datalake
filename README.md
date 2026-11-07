# Time-partitioned ride data lake

A local PySpark + Delta Lake exercise using synthetic ride data. The design
validates source records, writes daily event-time partitions, builds a city
dimension with SCD Type 2, and provides pruning and compaction demonstrations.

**Status:** The remaining pipeline, Delta writes, tests and demos were authored
without execution on the company laptop at the owner's request. They must be
run and debugged on a personal machine before the solution can be claimed as
working or submitted. Earlier manual checks of ingestion, validation and Spark
transformations are not a substitute for an end-to-end test.

## Run in GitHub Codespaces (recommended)

1. Create a **dedicated public GitHub repository** containing the contents of
   this project folder at the repository root; push the files. Do not publish
   secrets, company code, private paths or personal data.
2. On GitHub, choose **Code -> Codespaces -> Create codespace on main**. Wait
   for the dev container's post-create setup to finish. It provisions Python 3.11,
   Java 17, `.venv`, and the Python requirements. The first build and first Delta
   run need internet access for container/features, pip and Maven artifacts.
3. Open the Codespaces terminal at the repository root and run:

   ```bash
   bash run_all.sh
   ```

The script creates a **new** isolated directory under `output/runs/` on each
invocation; it does not overwrite previous runs. It generates data, runs the
pipeline twice with the same input and cutoff, compares counts and practical
table fingerprints, runs compaction and pruning demos, and runs pytest. If any
stage fails the script stops and points to that run's `logs/`. A successful run
prints the output location; inspect its `reconciliation/`, `logs/`, and
`pytest-results.xml`. Fingerprints are non-cryptographic checks and do not prove
table equality in all cases. Do not claim success before seeing results.

Codespaces is an external GitHub service; included usage and billing depend on
your account and current GitHub limits. Check your quota and billing settings
first if zero cost is essential, and stop/delete the Codespace when finished.
See `docs/TROUBLESHOOTING.md` for setup and failure diagnostics.

## Alternative: environment and manual setup

Use a personal machine running **Linux, macOS, or WSL2 on Windows**. WSL2 runs
the project in Linux and avoids native Windows `winutils.exe`/`hadoop.dll`
setup. Do not perform these steps on a restricted company machine. Use Python
3.10-3.12 (3.12 recommended), Java 17, Git and an internet connection for the
first installation. Spark uses `local[*]`; no cloud account or paid service is
required. Delta's JVM dependencies are fetched from Maven on first startup,
even after pip installation; offline operation requires a populated local cache.

1. Clone the public repository on the personal laptop and open a terminal in
   its root (the directory containing this README and `requirements.txt`).
2. Verify Python and Java: `python3 --version` and `java -version`.
   Install a compatible Python and Java 17 through the platform's trusted
   package manager if needed. On Ubuntu/WSL, for example, use the distribution's
   `openjdk-17-jdk`, Python 3.12 and Python venv packages. Set `JAVA_HOME` to
   the Java 17 installation. On macOS, `/usr/libexec/java_home -v 17` can help
   find the installed JDK.
3. Create an isolated environment and install dependencies:

   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   ```

4. Inspect `config.yaml`. Set `validation.as_of_utc` to an ISO-8601 UTC cutoff
   at or before the first run, and keep it fixed when retrying the same batch.
   The supplied fixed cutoff is for the 2024 synthetic sample, not a live clock.
   Input timestamps after the cutoff are rejected even if now in the past.
5. The 1,000-row `sample_data/small/rides.csv` and separate expected counts are
   included. Generate another sample only when desired. Existing files are
   protected: use `--force` only if replacement is intentional.

   ```bash
   python src/generate_data.py --size 100000 \
     --output sample_data/large/rides.csv \
     --expected-output sample_data/expected/large_expected.json
   ```

   To run the larger file, change `paths.source` in `config.yaml`. The generator
   uses seed 42 and simulates arrival order by putting late events at the end of
   the CSV. Expected counts describe injected defects, not proof of correctness.
6. Run the pipeline and automated checks:

   ```bash
   python src/run_pipeline.py
   pytest -q
   ```

7. Demonstrate pruning and compaction separately:

   ```bash
   python src/prune_demo.py --day 2024-01-01
   python src/optimize.py
   ```

   Choose a date present in the data for a meaningful pruning comparison. The
   optimization script creates an isolated demo table under the configured
   `paths.compaction_benchmarks`; it does not compact production tables.

## Architecture and output

`CSV -> ingest -> validate -> Bronze -> Silver -> Gold fact + city dimension`

Invalid rows are written to `data/quarantine/<batch fingerprint>/`. Bronze and
Silver use `year/month/day` UTC event-time partitions. Gold contains an
unpartitioned fact table and a city dimension whose versions use left-closed,
right-open `[valid_from, valid_to)` intervals. Logs and reports are written to
`output/run_logs/` and `output/reconciliation/`; every run has a unique ID.

The report reconciles source = rejected + accepted Bronze rows for the current
batch, and compares batch Bronze/Silver/Gold counts and fare totals. Table totals
also include earlier batches. The run log includes start/end, counts, status,
failure reason and validation cutoff. The cutoff and source-content fingerprint
help reproduce a retry. For an empty or all-rejected batch, target tables are
not changed; log target counts describe the batch, not existing table totals.

`prune_demo.py` records executed Spark SQL metrics where available, the physical
plan, files emitting rows and their actual filesystem sizes. Those sizes are
**not physical bytes transferred**; files emitting no rows are not counted in
that file list. Verify `PartitionFilters` and any file-count/file-size scan
metrics in the plan. `optimize.py` reports active Delta file count and mean size before/after
OPTIMIZE on a deliberately fragmented single-date demo. No performance result is
claimed until these scripts have been run on the personal machine.

## Data dictionary and naming

All fields and tables use `snake_case`; input timestamps are ISO-8601 with an
explicit UTC offset. Source input is synthetic and contains no expected labels.

| Source field | Type | Rules / meaning |
| --- | --- | --- |
| `ride_id` | string | Required; unique ride/event identifier under this project assumption. |
| `rider_id`, `driver_id` | string | Required; no separate rider/driver reference dataset is provided. |
| `city` | string | Required; Mumbai, Delhi, Bangalore, Hyderabad or Chennai. |
| `fare` | number as CSV text | Must be finite and greater than zero; interpreted as INR. |
| `event_time` | ISO-8601 string | Required, parseable with offset and not later than configured cutoff. |
| `status` | string | `completed`, `cancelled`, or `in_progress`. |

Ingestion adds `run_id`, source filename and `_corrupt_record`. Invalid rows
retain those fields plus `rejection_reason`; the first failing rule wins. City
membership uses the configured list; it is not a lookup against an external
reference table. Duplicate detection keeps one deterministic valid row per
`ride_id` within a batch; invalid rows cannot displace valid rows.

Bronze retains raw values plus `event_timestamp`, `year`, `month`, `day`.
Silver casts fare to double and replaces the raw time string with a timestamp.
Gold fact has `ride_id`, `rider_id`, `driver_id`, `city_key`, `fare`, `event_time`.
The city dimension has deterministic `city_key`, `city_name`, `region`,
`valid_from`, `valid_to`, and `is_current`. Initial city/region mappings and
effective times are in `config.yaml`. First bootstrap must predate the earliest
ride. The fixed example regions are synthetic assumptions, not authoritative
geography data. Update the effective timestamp deliberately when changing a
region; backdated changes to an existing current version are rejected.

## Idempotency, retry, backfill and limitations

Delta merges use `ride_id` and ignore changes to run/source metadata on exact
replay. Conflicting payloads for an existing ID fail rather than overwrite.
Consequently the current implementation assumes **one event per ride**; it does
not support multiple changing events for the same ride or arbitrary corrections.
Introduce a stable event key and a separate event-history design for that case.

Run status and quarantine are persisted separately from target tables. Each
individual Delta commit is atomic, but there is no transaction across tables,
reports or logs. A failed run can leave a partially updated project. Inspect the
failure log, correct the issue, and rerun the identical input and cutoff; previously
committed identical ride rows are not inserted again. An initial failed write
may leave a non-Delta directory; the writer refuses to delete or overwrite it.
Inspect and resolve it manually before retrying. Concurrent writers and concurrent
first-time table creation are unsupported.

For a backfill, use a different source file with new immutable ride IDs; records
are routed to their event dates. The city history must cover those dates. A batch
with a conflicting old ID, retroactive dimension correction, or dimension gap
fails instead of silently changing history. Quarantine uses a content/rule
fingerprint and is overwritten on a retry of the same logical input; verify
permissions and available space. Empty files and unexpected headers are handled
by ingestion, but row-level malformed CSV behavior depends on Spark's parser.

Daily partitioning helps date filters but small daily volumes may create tiny
files; the 128 MiB config value is a target, not a promise. Compaction does not
combine days. `docs/trade_off_note.md` explains query and sizing trade-offs.

## Submission and transparency

Record **every** project-related AI prompt from every tool in
`prompts/prompt_history.md`, including prompts already used; responses are
optional. Review the prefilled transcript and add any unavailable attachment
content or other AI conversations. Explain personal
review and validation honestly. Inspect all generated artifacts before making
the repository public: never commit credentials, personal paths or proprietary
data. Some demo reports can contain local absolute paths; redact or replace them
before publishing while preserving useful traceability. Include actual test,
reconciliation and performance evidence from the personal laptop—do not submit
unexecuted tests or hypothetical numbers as verified results.
