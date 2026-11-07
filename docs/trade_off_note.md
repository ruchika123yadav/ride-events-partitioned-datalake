# Partitioning and file-size strategy

This note describes design choices; measured reports must be generated locally.

## Decision: UTC daily event-time partitions

Bronze and Silver use `year/month/day` derived from the ride's event time, not
the processing date. Date-range reports can filter all three partition columns.
Late data with a new ride ID goes to its historical date. Same-ID conflicts are
rejected; they are not silently overwritten. Gold facts are unpartitioned.

| Layout | Advantage | Cost and suitable workload |
| --- | --- | --- |
| Daily (chosen) | Day filters can prune other days; a practical date-based access path. | Low daily volume can create tiny files. The starter sample is deliberately sparse. |
| Monthly | Fewer partitions/files at low volume. | A one-day query generally selects the month partition; Parquet row-group statistics may still skip data within its files. |
| Hourly | Finer pruning for hour-level queries. | At low volume, many tiny files and metadata overhead; suitable only when hourly volumes justify it. |

This is a workload-dependent starting decision, not a claim that daily files
are healthy-sized for every input. Queries by rider or driver without a date
filter gain little from daily partitioning, and excessive partitions can slow
planning. Real measurements and representative production distributions should
drive any change to granularity or clustering.

## File sizing and compaction

`config.yaml` sets a 128 MiB initial compaction target, not a guarantee. Small
days cannot become large without combining separate days, which this design does
not do. Delta OPTIMIZE combines eligible files *within* a partition. The isolated
`src/optimize.py` demonstration deliberately writes several files into one date
and reports active file count and average file size before/after. It does not
compact production tables; review partition sizes and operational impact first.

## Evidence and limitations

Run `src/prune_demo.py --day YYYY-MM-DD` after the pipeline. It reports rows,
the distinct `input_file_name()` files that emit rows, their actual filesystem
sizes, elapsed time, executed SQL metrics when available, and the physical plan.
Inspect `PartitionFilters` and available scan metrics. Summed on-disk file sizes
are **not physical bytes transferred**: empty files and files emitting no rows
are excluded, and caching or Parquet column/row-group pruning may affect I/O.
Spark's reported metrics depend on the execution plan and version; missing
metrics are not fabricated. No speedup or file reduction is claimed until
measured. These demos have not been executed in the current authoring environment.

## Reliability trade-offs

Each Delta table commit is atomic, but the pipeline updates several tables and
reports separately. A failure may leave a partial run; the documented retry
uses deterministic keys, immutable ride merges and a full city-history snapshot.
There is no cross-table transaction or concurrent-writer coordination. Same-ID
changed ride payloads and retroactive city-region changes fail explicitly. To
support multiple ride events or corrections, introduce a stable event ID and
define event history and correction semantics before changing merge behavior.
