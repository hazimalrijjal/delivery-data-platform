"""Load one batch of CSV files into BigQuery dataset `raw` (same layout as ingest/load_raw.py does for DuckDB).

    set GCP_PROJECT=your-project-id
    gcloud auth application-default login
    python ingest/load_bigquery.py --batch-dir data/raw/batch_001 --mode replace

Modes
  replace  (default) each raw table is replaced by this batch (load job with WRITE_TRUNCATE).
           Safe to run again and works in the free BigQuery sandbox, because it uses no DML.
  append   deletes this batch's old rows with a DELETE statement, then appends the batch.
           Keeps several batches side by side, but DML needs a project with billing enabled.

All source columns are loaded as STRING, plus _batch_id, _source_file (STRING) and _loaded_at (TIMESTAMP).
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

TABLES = ["customers", "merchants", "couriers", "orders", "order_events"]
DATASET = "raw"


def read_batch_table(csv: Path, batch_id: str) -> pd.DataFrame:
    # keep_default_na=False would turn empty fields into '' ; the DuckDB loader gives NULL for empty fields, so do the same here
    df = pd.read_csv(csv, dtype=str)
    df["_batch_id"] = batch_id
    df["_source_file"] = csv.name
    df["_loaded_at"] = pd.Timestamp(datetime.now(timezone.utc))
    return df


def load_batch(client, project: str, batch_dir: Path, mode: str = "replace", dataset: str = DATASET) -> dict[str, int]:
    from google.cloud import bigquery
    if mode not in ("replace", "append"):
        raise ValueError("mode must be 'replace' or 'append'")
    batch_id = batch_dir.name
    ds = bigquery.Dataset(f"{project}.{dataset}")
    client.create_dataset(ds, exists_ok=True)
    counts: dict[str, int] = {}
    for table in TABLES:
        csv = batch_dir / f"{table}.csv"
        if not csv.exists():
            raise FileNotFoundError(csv)
        df = read_batch_table(csv, batch_id)
        table_id = f"{project}.{dataset}.{table}"
        schema = [bigquery.SchemaField(c, "TIMESTAMP" if c == "_loaded_at" else "STRING") for c in df.columns]
        if mode == "append":
            client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
            client.query(f"delete from `{table_id}` where _batch_id = @b",
                         job_config=bigquery.QueryJobConfig(query_parameters=[
                             bigquery.ScalarQueryParameter("b", "STRING", batch_id)])).result()
            disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:
            disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
            schema=schema, write_disposition=disposition))
        job.result()                                    # wait; raises if the load failed
        counts[table] = len(df)
    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batch-dir", type=Path, required=True)
    p.add_argument("--mode", choices=["replace", "append"], default="replace")
    p.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
    a = p.parse_args()
    if not a.project:
        raise SystemExit("set GCP_PROJECT (or pass --project)")
    from google.cloud import bigquery
    client = bigquery.Client(project=a.project)
    for t, n in load_batch(client, a.project, a.batch_dir, a.mode).items():
        print(f"{a.project}.raw.{t:14}{n:>10,} rows")


if __name__ == "__main__":
    main()
