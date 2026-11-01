"""The BigQuery loader's logic, tested with a fake client (no network, no Google account).
This checks WHAT the loader asks BigQuery to do. It does not prove BigQuery accepts it: see docs/BIGQUERY.md."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("google.cloud.bigquery")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingest"))
import load_bigquery  # noqa: E402

CSVS = {"customers": "customer_id,zone_id\nC1,1\nC2,\n", "merchants": "merchant_id\nM1\n", "couriers": "courier_id\nR1\n",
        "orders": "order_id,customer_id\nO1,C1\nO2,\n", "order_events": "event_id\nE1\n"}


@pytest.fixture
def batch(tmp_path):
    d = tmp_path / "batch_007"
    d.mkdir()
    for name, text in CSVS.items():
        (d / f"{name}.csv").write_text(text)
    return d


def test_replace_mode_uses_truncate_and_no_dml(batch):
    from google.cloud import bigquery
    client = MagicMock()
    counts = load_bigquery.load_batch(client, "proj", batch, mode="replace")
    assert counts["orders"] == 2 and set(counts) == set(load_bigquery.TABLES)
    client.query.assert_not_called()                       # no DELETE: works in the sandbox
    for call in client.load_table_from_dataframe.call_args_list:
        assert call.kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    df = client.load_table_from_dataframe.call_args_list[0].args[0]
    assert set(df["_batch_id"]) == {"batch_007"} and "_loaded_at" in df.columns


def test_append_mode_deletes_the_batch_first(batch):
    from google.cloud import bigquery
    client = MagicMock()
    load_bigquery.load_batch(client, "proj", batch, mode="append")
    assert client.query.call_count == len(load_bigquery.TABLES)
    sql = client.query.call_args_list[0].args[0]
    assert sql.startswith("delete from `proj.raw.customers`") and "_batch_id = @b" in sql
    for call in client.load_table_from_dataframe.call_args_list:
        assert call.kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_APPEND


def test_empty_csv_fields_become_null(batch):
    df = load_bigquery.read_batch_table(batch / "orders.csv", "b")
    assert df["customer_id"].isna().sum() == 1


def test_schema_is_all_string_plus_loaded_at(batch):
    client = MagicMock()
    load_bigquery.load_batch(client, "proj", batch)
    schema = client.load_table_from_dataframe.call_args_list[0].kwargs["job_config"].schema
    types = {f.name: f.field_type for f in schema}
    assert types["_loaded_at"] == "TIMESTAMP" and types["customer_id"] == "STRING" and types["_batch_id"] == "STRING"


def test_missing_file_and_bad_mode(batch):
    (batch / "orders.csv").unlink()
    with pytest.raises(FileNotFoundError):
        load_bigquery.load_batch(MagicMock(), "proj", batch)
    with pytest.raises(ValueError):
        load_bigquery.load_batch(MagicMock(), "proj", batch, mode="nope")
