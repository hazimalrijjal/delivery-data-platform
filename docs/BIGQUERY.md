# Running the models on BigQuery

**Status: the author ran this on a free BigQuery sandbox (week 3). Afterwards money columns changed from floating point to exact decimals** (see docs/SPARK_AND_STREAMING.md), so repeat steps 7 and 8 below and check the numbers again: the expected commission total is now **709,496.31** (it was 709,494.76 with floats; BigQuery's `ROUND` on `FLOAT64` is likely to have given yet another value).
The tooling that wrote this code cannot reach Google Cloud; everything below was checked by compiling, parsing and unit tests only.

## What was checked (without BigQuery)

| Check | Result |
|---|---|
| All models and tests compiled for the BigQuery adapter (`dbt compile`) | all 53 files render |
| Each compiled file parsed with a BigQuery SQL parser (sqlglot) | 0 syntax errors |
| DuckDB results before and after making the SQL portable | identical (checksums + 90 days of KPIs recomputed with the original SQL) |
| Loader logic against a fake client (`tests/test_load_bigquery.py`) | 5 tests pass |

A parser proves syntax, not meaning. Reading the rendered SQL found and fixed two real BigQuery problems before any run
(a TIMESTAMP compared with a DATETIME in the incremental filter; NULLs in the percentile).

## How one codebase runs on two engines

Engine differences are isolated in `dbt_project/macros/portable.sql` (`try_cast_timestamp`, `p90`, `epoch_day`, `ts_minus_days`, `type_double`,
`fct_orders_materialization`, `incremental_strategy_for_target`) plus dbt's own `dbt.datediff`, `dbt.date_trunc`, `dbt.type_numeric`.
dbt picks `bigquery__name` or `default__name` (the DuckDB version) from the adapter in use: `adapter.dispatch`.

| Difference | DuckDB | BigQuery |
|---|---|---|
| unparseable text to timestamp | `try_cast` | `safe_cast` |
| 90th percentile | `quantile_cont` (exact) | `approx_quantiles` (approximate) |
| RANGE window over days | date minus 1970-01-01 | `unix_date` (RANGE needs a number) |
| timestamp minus N days | `ts - interval N day` | `timestamp_sub` |
| incremental strategy | `delete+insert` | `merge` |
| decimal / 64-bit float | `decimal`, `double` | `numeric`, `float64` |

## The free sandbox has no DML

Google documents that the BigQuery sandbox does not support DML (INSERT, UPDATE, DELETE, MERGE). Consequences, already handled:
- `fct_orders` is incremental (MERGE) on target `bq` and a plain table on target `bq_sandbox`.
- The loader's default `--mode replace` replaces each raw table with a load job (no DML). `--mode append` uses DELETE and needs billing.
- Sandbox tables expire after 60 days and storage is limited to 10 GiB in total. This project uses about 25 MB.
Whether `CREATE TABLE AS SELECT` (what dbt uses) is accepted in the sandbox is **not confirmed**; it is the first thing to watch for.

## Steps (Windows cmd, venv active)

1. Google Cloud console: create a project (stay in sandbox mode, no billing). Note the project id.
2. Install the Google Cloud CLI, then: `gcloud auth application-default login`
3. `pip install -r requirements-bq.txt`
4. `set GCP_PROJECT=your-project-id` and `set BQ_LOCATION=US`
5. `python ingest\load_bigquery.py --batch-dir data\raw\batch_001 --mode replace`
6. `cd dbt_project`, then `dbt debug --target bq_sandbox --profiles-dir .` (should end with "All checks passed")
7. `dbt build --target bq_sandbox --profiles-dir .` (expect PASS=53 like DuckDB)
8. Paste each query of `scripts/bq_verify.sql` into the BigQuery console and compare with the expected values in its comments.

If any step fails, copy the full error text. A failure here is useful: it shows exactly which assumption was wrong.
