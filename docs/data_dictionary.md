# Data Dictionary and Relationships

All timestamps use ISO-8601 with an explicit offset in CSV; Spark processes them
in UTC. Names use `snake_case`. Required fields are enforced by validation; Delta
schemas may still mark columns nullable, so these are application-level rules.

## Source: ride event

**Grain:** one row per ride under the current immutable `ride_id` contract.
Every accepted `ride_id` is unique within the dataset. The same ID with a
different payload across runs is a conflict, not a new event version.

| Field | Source type | Nullability and business rules | Key / relationship |
| --- | --- | --- | --- |
| `ride_id` | string | Required and nonblank. | Logical primary key; duplicates are rejected within a batch. |
| `rider_id` | string | Required and nonblank. | Identifier only; no rider master lookup is implemented. |
| `driver_id` | string | Required and nonblank. | Identifier only; no driver master lookup is implemented. |
| `city` | string | Required; must be in the configured city list. | Resolves to an effective city dimension version for Gold. |
| `fare` | numeric text | Required, finite and greater than zero; measured in INR. | No separate reference entity. |
| `event_time` | ISO-8601 timestamp text | Required, parseable with timezone and not later than the fixed run cutoff. | Drives UTC partitions and the city as-of join. |
| `status` | string | Required; `completed`, `cancelled`, or `in_progress`. | No separate reference entity. |

## Derived fields and targets

| Entity / field | Type | Rule |
| --- | --- | --- |
| Bronze `event_timestamp` | timestamp | Parsed from `event_time` in UTC; required for accepted rows. |
| Bronze/Silver `year`, `month`, `day` | integer | Derived from event time; determine the daily Delta partition. |
| Bronze/Silver `run_id` | string | Unique execution identifier, not an entity key; preserved on replay. |
| Bronze/Silver `source_file` | string | Input filename; paired with run metadata and batch fingerprint for tracing. |
| Bronze `_corrupt_record` | string | Spark's original malformed CSV text when available; accepted rows do not have a parser error. |
| Silver `fare`, `event_time` | double, timestamp | Cast from validated input; one selected record per ride ID. |
| Gold `ride_fact.ride_id` | string | Unique ride key under the immutable source contract. |
| Gold `ride_fact.city_key` | string | Required city-version reference; no match or multiple matches fail the build. |
| Gold `ride_fact.rider_id`, `driver_id`, `fare`, `event_time` | string, string, double, timestamp | Derived from validated Silver records; no separate rider/driver foreign keys. |
| `city_dim.city_key` | string | Stable SHA-256 derived from city, region and effective start; unique per version. |
| `city_dim.city_name`, `region` | string | Required; mappings in config are synthetic reference assumptions. |
| `city_dim.valid_from`, `valid_to` | timestamp | Required start; end is null for the current version and populated for a closed version. |
| `city_dim.is_current` | boolean | Exactly one current version per city; no overlapping effective intervals. |

Each ride fact references the city version whose validity interval contains its
event time: `[valid_from, valid_to)`. The initial city snapshot must start before
the earliest ride. The current design does not enforce a rider or driver master
foreign key because those reference entities are not supplied.

## Rejected records and generated labels

Quarantined rows retain source fields, `run_id`, `source_file`, parser-provided
`_corrupt_record` where applicable, and the first `rejection_reason`. The run
log includes a source-content/rule fingerprint. Normal CSV parsing does not
capture a physical line number for each quarantined row.

For synthetic input, `expected/record_labels.csv` separately records the CSV
line number, ride ID, injected defect and simulated late-arrival flag. Line 1 is
the header. These labels describe what the generator injected, **not** the
pipeline's final validation decision; either member of a duplicate-ID pair may
be selected by the deterministic duplicate rule.
