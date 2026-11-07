# Design Review

## Architecture

The pipeline uses local Spark and Delta Lake to validate synthetic ride events,
partition Bronze and Silver by UTC event date, and publish a ride fact with an
effective-dated city dimension. Invalid rows are retained in quarantine.

## Design decisions

- Immutable ride IDs make identical replays safe and surface conflicting payloads.
- City versions use left-closed, right-open validity intervals for as-of joins.
- Daily partitions favor date filters; low-volume days can still create small files.
- The compaction demonstration is isolated from production tables.
- Reconciliation checks batch counts and fare totals across layers.
- Separate injection labels describe generated defects without modifying input.
- Isolated reviewer demonstrations exercise late-arriving batches and city history.

## Operational considerations

Delta commits are atomic per table, not across the pipeline. Retry partial runs
with the same input and cutoff after reviewing the run log. Concurrent writers,
arbitrary ride corrections, and backdated city changes require additional design.
Pruning file sizes describe observed files, not physical I/O transferred.

## Compatibility

Delta Lake 3.3.x supports Spark 3.5.x. The project pins `delta-spark==3.3.3`
and `pyspark==3.5.9`; Python 3.11 and Java 17 are configured for Codespaces.
See the [Delta Lake compatibility matrix](https://docs.delta.io/releases/).
