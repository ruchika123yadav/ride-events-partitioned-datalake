# Attached build prompt (message 11 in prompt_history.md)

```text
# PROJECT: Time-Partitioned Data Lake with Partitioning & File-Sizing Strategy

You are helping me build a Data Engineering project. Build it step by step, explain each file's core logic as you go (I need to understand it, not just copy it). Ask me before moving to the next module so I can follow along.

## TECH STACK (fixed — do not change)
- Python 3.10+
- PySpark in LOCAL mode (master="local[*]") — NOT Databricks, NOT cloud
- Delta Lake (delta-spark package) for the storage layer
- pytest for automated tests
- PyYAML for config
Everything must run on a local laptop with zero cloud dependencies and zero cost.

## BUSINESS DOMAIN
Ride-booking events (like Uber/Ola). Each record is one ride event.
Schema:
- ride_id      : string, unique per ride (e.g. "RIDE_00042")
- rider_id     : string
- driver_id    : string
- city         : string (one of: Mumbai, Delhi, Bangalore, Hyderabad, Chennai)
- fare         : float, must be > 0 (INR)
- event_time   : ISO-8601 timestamp (when the ride happened)
- status       : string (completed / cancelled / in_progress)

## WHAT THE PROJECT MUST DO (the core objective)
Design a partitioning and file-sizing strategy for a time-partitioned data lake.
Store data date-wise (year/month/day), demonstrate partition PRUNING (queries skip
irrelevant partitions), avoid the small-files problem via Delta OPTIMIZE compaction,
and document trade-offs. The pipeline must validate data, quarantine bad records,
be idempotent on rerun, and produce measurable, reconcilable outputs.

## PARTITIONING DECISION (implement exactly this)
Use DAILY partitioning (year=/month=/day=). In the trade-off note, explain:
- Daily: good pruning, healthy file sizes for this data volume → CHOSEN
- Monthly: fewer files but weak pruning (reads whole month for one day)
- Hourly: precise pruning but causes small-files problem at low volume (only suitable for Uber-scale hourly volume)
Handle late-arriving/out-of-order data by routing each record to the partition of its
event_time (not the processing time).

## EXACT FOLDER STRUCTURE (create this)
partitioned-datalake/
├── README.md                  (architecture, data dictionary, setup, how to run)
├── requirements.txt
├── config.yaml                (all paths, partition granularity, file-size target, sample size)
├── src/
│   ├── generate_data.py       (synthetic data generator, see rules below)
│   ├── ingest.py              (read raw CSV, stamp run_id + source_file on every row)
│   ├── validate.py            (schema + business-rule checks, split valid vs quarantine)
│   ├── transform.py           (Bronze → Silver → Gold, with SCD Type 2)
│   ├── partition_writer.py    (partitioned Delta writes by year/month/day)
│   ├── optimize.py            (compaction demo: measure files + avg size before/after OPTIMIZE)
│   ├── prune_demo.py          (pruning demo: full scan vs filtered, measure files/bytes read)
│   └── run_pipeline.py        (orchestrates everything, writes the run log)
├── tests/
│   └── test_pipeline.py       (8 pytest cases, listed below)
├── sample_data/
│   ├── small/                 (1,000-row starter sample)
│   └── expected/              (expected counts, kept SEPARATE from input data)
├── output/
│   ├── run_logs/              (one JSON per run)
│   └── reconciliation/        (reconciliation report)
├── prompts/
│   └── prompt_history.md      (I will paste all AI prompts here — create the file)
└── docs/
    └── trade_off_note.md      (daily vs monthly vs hourly comparison + chosen strategy)

## DATA GENERATOR RULES (generate_data.py)
- Use random.seed(42) so data is fully reproducible
- Generate a configurable row count (default 1,000; support 100,000+ via a --size arg)
- Spread event_time across a full year (2024) so multiple daily partitions are created
- DELIBERATELY inject bad data so edge cases can be demonstrated:
  * ~5% rows with NULL city or NULL event_time
  * ~3% duplicate ride_id
  * ~2% rows with fare <= 0
  * ~1% late-arriving rows (event_time a few days in the past relative to others)
  * a few rows with an invalid city not in the allowed list
- Keep expected-answer labels (expected valid/rejected counts) in sample_data/expected/, NOT mixed into the input

## VALIDATION RULES (validate.py) — each rejected row keeps a rejection_reason
- ride_id NULL        → reject, reason "missing_ride_id"
- event_time NULL     → reject, reason "missing_event_time"
- fare <= 0           → reject, reason "invalid_fare"
- city not in allowed → reject, reason "invalid_city"
- duplicate ride_id   → reject the duplicate, reason "duplicate_ride_id"
- event_time in future→ reject, reason "future_timestamp"
Output two datasets: valid records, and quarantine records (with rejection_reason,
run_id, source_file). Save quarantine to data/quarantine/.

## TRANSFORM RULES (transform.py)
- BRONZE: write valid records as Delta, partitioned by year/month/day (raw but partitioned)
- SILVER: read Bronze, remove remaining duplicates (window on ride_id, keep latest), fix data types
- GOLD: build a star schema:
    * ride_fact (ride_id, rider_id, driver_id, city_key, fare, event_time)
    * city_dim (city_key, city_name, region) with SCD Type 2 — when a city's region
      changes, close the old row (end_date + is_current=false) and insert a new current row

## IDEMPOTENCY (run_pipeline.py / transform.py)
Use Delta MERGE on ride_id so rerunning the same input does NOT create duplicates.
Document retry, restart, and backfill behavior in the README.

## LOGGING (every run writes output/run_logs/RUN_<timestamp>.json)
Fields: run_id, start_time, end_time, input_count, valid_count, rejected_count,
bronze_count, silver_count, gold_fact_count, status, failure_reason.

## RECONCILIATION (printed at end + saved to output/reconciliation/)
Assert and display: source_count == quarantine_count + bronze_count.
Show silver vs bronze difference (dedup removed) and gold vs silver. Explain any gaps.

## PERFORMANCE DEMOS (the standout features — make the numbers visible)
1. prune_demo.py: run a full-scan count (no filter) vs a single-day filtered count.
   Print files read and bytes scanned for each, and the % reduction from pruning.
2. optimize.py: show file count + average file size BEFORE and AFTER Delta OPTIMIZE
   compaction, and the % file reduction.

## AUTOMATED TESTS (tests/test_pipeline.py — 8 cases)
1. Happy path (clean records flow through to gold)
2. Empty input (pipeline doesn't crash)
3. Malformed record (goes to quarantine)
4. Duplicate key (only one survives in gold)
5. Null handling (null required field → quarantine with reason)
6. Late-arriving data (routed to the correct historical partition)
7. Invalid reference / invalid city (quarantined)
8. Rerun idempotency (running same input twice does not double counts)

## UNIQUENESS / THINGS THAT MUST STAND OUT
- Measurable before/after numbers for BOTH pruning and compaction (not just claims)
- Late-arriving data handled correctly (partition by event_time, not processing time)
- SCD Type 2 on the city dimension
- A clean trade_off_note.md comparing daily/monthly/hourly with a clear recommendation
- Rerun idempotency demonstrated with actual counts
- Clear, modular, readable code with configurable paths via config.yaml

## DELIVERY RULES
- Everything runnable from a clean environment with: pip install -r requirements.txt, then python src/run_pipeline.py
- No secrets, no cloud credentials, no external data — fully synthetic and local
- Write a concise README with: architecture/flow description, data dictionary, setup steps, how to run, known limitations, and next steps
- Explain the core logic of each module briefly as you build it

## HOW TO PROCEED
Start with Step 1: project skeleton + requirements.txt + config.yaml + README outline.
Then build modules one at a time in this order: generate_data → ingest → validate →
transform (bronze/silver/gold) → partition_writer → optimize → prune_demo →
run_pipeline → tests. After each module, briefly explain what it does and pause so I
can confirm before you continue.
```
