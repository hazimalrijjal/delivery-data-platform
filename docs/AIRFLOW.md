# Orchestration with Airflow

`dags/delivery_pipeline.py` (Airflow 3) runs the same steps as `scripts/run_pipeline.bat`, but with ordering, retries, a schedule and a quality gate:

```
wait_for_batch  ->  load_raw  ->  dbt_build  ->  dq_gate
 (sensor)          (idempotent)  (53 checks)    (reads marts.dq_report)
```

| Choice | Why |
|---|---|
| `PythonSensor(mode="reschedule")` | waits for the batch files without holding a worker slot |
| `load_raw` is idempotent | a retry or a manual re-run of the same batch cannot duplicate rows |
| `retries=2` on loading, `retries=0` on `dq_gate` | a locked file may fix itself, bad data does not; retrying would only hide it |
| `dbt build` = models + tests in dependency order | one failing test stops the DAG before the gate |
| `dq_gate` thresholds in `pipeline/tasks.py` | excluded rows over 3% or repaired rows over 10% fail the run |
| `catchup=False`, `max_active_runs=1` | no flood of back-runs; DuckDB has one writer |

The task logic is plain Python in `pipeline/tasks.py` and is tested without Airflow (`tests/test_tasks.py`).

## What was verified

Run on Airflow 3.3.2 (Linux) with `airflow dags test`:
- **Success path:** empty warehouse, all four tasks succeed, dbt `PASS=53`, run marked successful.
- **Failure path:** a batch with 8% missing customers made `dq_gate` fail with `null_customer: 8.001% is over the 3.0% limit`; the run was marked failed and the gate was not retried.
- `tests/test_dag_structure.py` checks task order, retry policy and schedule settings (runs only where Airflow is installed).

**Not verified:** the Docker file `docker-compose.airflow.yml`, the web UI, and any run on Windows. Airflow does not run natively on Windows: use Docker Desktop, WSL2, or skip it and run `scripts\run_pipeline.bat`, which does the same steps without scheduling.

## Known limit

The demo data is one static batch, so the daily schedule re-loads the same batch. In a real setup the batch folder name would come from the run date (`{{ ds_nodash }}`) and files would arrive from a source system.
