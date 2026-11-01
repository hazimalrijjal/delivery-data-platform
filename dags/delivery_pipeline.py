"""Daily pipeline: wait for the batch -> load raw -> dbt build (staging, marts, tests) -> data-quality gate.

The work itself lives in pipeline/tasks.py and dbt_project/; this file only describes ORDER, RETRIES and SCHEDULE.
Written for Airflow 3 (airflow.sdk).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, Param, task

ROOT = Path(os.environ.get("PLATFORM_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT))
from pipeline import tasks  # noqa: E402

DB_PATH = ROOT / "warehouse" / "delivery.duckdb"

default_args = {
    "owner": "data-engineering",
    "retries": 2,                         # a temporary problem (file still being copied, locked file) usually fixes itself
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="delivery_pipeline",
    description="Raw load, dbt build and data-quality gate for the delivery platform",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,                         # do not run every missed day when the DAG is first switched on
    max_active_runs=1,                     # DuckDB allows one writer at a time
    default_args=default_args,
    params={"batch_name": Param("batch_001", type="string", description="folder under data/raw/ to load")},
    tags=["delivery", "dbt", "duckdb"],
) as dag:

    wait_for_batch = PythonSensor(
        task_id="wait_for_batch",
        python_callable=lambda params, **_: tasks.batch_ready(ROOT / "data" / "raw" / params["batch_name"]),
        poke_interval=30,
        timeout=60 * 30,
        mode="reschedule",                 # frees the worker slot between checks
    )

    @task
    def load_raw(params=None):
        counts = tasks.load_batch_to_raw(ROOT / "data" / "raw" / params["batch_name"], DB_PATH)
        for table, n in counts.items():
            print(f"raw.{table}: {n:,} rows")
        return counts

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd '{ROOT / 'dbt_project'}' && dbt build --profiles-dir .",
        # dbt exits with an error code when any model or test fails, which fails this task and stops the DAG
    )

    @task(retries=0)                       # bad data does not fix itself, so retrying would only hide the problem
    def dq_gate():
        summary = tasks.check_data_quality(DB_PATH)
        for line in summary:
            print(line)

    wait_for_batch >> load_raw() >> dbt_build >> dq_gate()
