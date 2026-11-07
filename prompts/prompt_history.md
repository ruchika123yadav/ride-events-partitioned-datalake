# AI-Assisted Development: Project Prompt Summary

## Disclosure

Codex assisted with requirements analysis, solution design, implementation,
debugging and documentation. The entries below are **professional English
paraphrases of requests made during this project**, not verbatim prompts or a
complete export. The error messages included below were actually reported.

The submission instructions request all prompts used across AI tools. This
summary alone is not a complete prompt history; the candidate should supply the
original project prompts or a complete record with any export limitations disclosed.

## 1. Understand the challenge

> Explain the problem statement and submission requirements in simple terms.
> What business problem does the project solve, and how does it relate to data
> engineering?

## 2. Design and build the project

> Build a local data-engineering project using Python, PySpark, Delta Lake,
> pytest and YAML configuration. Use synthetic ride-booking events and a modular
> pipeline that can be explained and reproduced.

## 3. Choose a partitioning strategy

> Use daily `year/month/day` partitions based on `event_time`. Compare daily,
> monthly and hourly partitioning for pruning, file size and common query
> patterns. How should late-arriving records be routed?

## 4. Generate representative data

> Create a reproducible data generator with a fixed seed and configurable size.
> Include missing values, duplicates, invalid fares, invalid cities and late
> records. Keep expected counts and labels separate from input data.

## 5. Implement ingestion and validation

> Read the CSV with an explicit schema and add `run_id` and `source_file`.
> Validate required fields, timestamps, fare, city and status. How should
> malformed, duplicate and invalid records be quarantined and traced?

## 6. Build analytical layers and preserve history

> Implement Bronze, Silver and Gold transformations. Build a ride fact and a
> city dimension. If a city's region changes, how can SCD Type 2 preserve the
> previous version and link historical rides to the correct version?

## 7. Make reruns safe and results auditable

> Use Delta MERGE to prevent duplicate records on replay. Record run status,
> counts and failures. Reconcile source, quarantine and target records, and
> explain retry, restart, backfill and transaction limitations.

## 8. Test correctness and measure performance

> Add automated tests for valid, empty, malformed, duplicate, null, late,
> invalid-reference, retry and replay cases. Demonstrate partition pruning
> and file compaction with measurable before/after outputs.

## 9. Prepare a reproducible execution environment

> Add a Codespaces configuration with Python 3.11 and Java 17, plus a single
> script that generates data, runs and replays the pipeline, executes the
> demonstrations and runs pytest. Save the results for review.

## 10. Troubleshoot reported setup errors

> The Codespace run stopped with:
>
> `ModuleNotFoundError: No module named 'yaml'`
>
> How can the project environment and dependencies be set up correctly?

The user subsequently reported Python 3.14, an `ensurepip` failure, and the
absence of Python 3.11 and `uv` in the original Codespace environment. A
container rebuild and dependency installation were then discussed.

## 11. Review actual execution results

> Review the pipeline output against the acceptance criteria. Do the source,
> quarantine and target counts reconcile? Did the replay, pruning, compaction
> and automated tests demonstrate the intended behaviour?

The candidate shared a `run01` report with 1,000 source records, 105 rejects,
895 records in each target layer and 20 passing tests. That run preceded a later
change to pytest's temporary-directory location.

## 12. Make reviewer evidence easier to understand

> Add clear demonstrations of a late-arriving batch and an SCD Type 2 change.
> Organise new execution attempts under `output/runs/run01`, `run02` and so on,
> while preserving earlier runs. Provide readable reports and a data dictionary.

## 13. Prepare reviewer-facing documentation

> Rewrite the README as a concise, professional project guide. Include the
> supplied architecture and partitioning diagrams, setup, data model, validation
> rules, measured results, tests, limitations and AI-use disclosure.

## 14. Resolve submission workflow questions

> Explain which generated artifacts should be included as evidence, what Git
> ignores, and how to stage and push the selected project files.

An actual push attempt reported that Git LFS was configured but `git-lfs` was
not found on the environment's path.

 