# Reviewer summary: run01

Created at UTC: 2026-11-07T18:42:42.492896+00:00

## Reconciliation and replay

| Metric | First execution | Replay |
| --- | --- | --- |
| Source | 1000 | 1000 |
| Quarantine | 105 | 105 |
| Bronze batch | 895 | 895 |
| Silver batch | 895 | 895 |
| Gold batch | 895 | 895 |

Idempotency: **passed**; counts and table fingerprints match. Fingerprints are non-cryptographic checks, not proof against all collisions.

## Partition pruning

| Scan | Rows | Files emitting rows | On-disk bytes of those files | Seconds |
| --- | --- | --- | --- | --- |
| Full | 895 | 721 | 2591524 | 2.753 |
| One day | 2 | 2 | 7219 | 0.878 |

Day: 2024-01-01. File reduction: 99.72%.

On-disk file sizes are not physical bytes transferred; files emitting no rows are excluded. Inspect pruning.json for the execution plan and available Spark SQL metrics.

## Compaction (isolated demo)

| Stage | Active files | Average file bytes |
| --- | --- | --- |
| Before | 4 | 1506.0 |
| After | 1 | 4622.0 |

File reduction: 75.0%. The demo does not compact production pipeline tables.

## Late-arriving event (isolated demo)

| Observation | Result |
| --- | --- |
| First batch event (UTC) | 2024-03-04T10:00:00Z |
| Later batch event (UTC) | 2024-02-29T10:00:00Z |
| Observed Delta partition | year=2024/month=2/day=29 |
| Rows before / after | 2 / 3 |
| Historical partition rows before / after | 1 / 2 |

The later batch adds an older event to an existing historical partition derived from event time.

## City history SCD Type 2 (isolated demo)

| Snapshot | City | Region | Valid from UTC | Valid to UTC | Current |
| --- | --- | --- | --- | --- | --- |
| before | Mumbai | West | 2023-01-01T00:00:00Z | — | True |
| after | Mumbai | West | 2023-01-01T00:00:00Z | 2025-01-01T00:00:00Z | False |
| after | Mumbai | New West | 2025-01-01T00:00:00Z | — | True |

| Ride | Matched historical region |
| --- | --- |
| DEMO_NEW | New West |
| DEMO_OLD | West |

Region names in this controlled example are illustrative.

## Automated tests

| Tests | Passed | Failed | Errors | Skipped |
| --- | --- | --- | --- | --- |
| 20 | 20 | 0 | 0 | 0 |

See `../pytest-results.xml` and `../logs/08-pytest.log` for test details.
