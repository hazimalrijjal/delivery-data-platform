"""Structure of the Airflow DAG. Skipped automatically when Airflow is not installed (it is optional: see README)."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS))
    assert not bag.import_errors, bag.import_errors
    return bag.get_dag("delivery_pipeline")


def test_tasks_and_order(dag):
    assert set(dag.task_ids) == {"wait_for_batch", "load_raw", "dbt_build", "dq_gate"}
    assert dag.get_task("wait_for_batch").downstream_task_ids == {"load_raw"}
    assert dag.get_task("load_raw").downstream_task_ids == {"dbt_build"}
    assert dag.get_task("dbt_build").downstream_task_ids == {"dq_gate"}


def test_retry_policy(dag):
    assert dag.get_task("load_raw").retries == 2
    assert dag.get_task("dq_gate").retries == 0          # bad data must not be retried


def test_schedule_settings(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
