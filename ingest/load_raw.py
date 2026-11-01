"""Load one batch of CSV files into the DuckDB raw layer.

    python ingest/load_raw.py --batch-dir data/raw/batch_001 --db warehouse/delivery.duckdb

Rules:
  * raw tables keep every source column as text (nothing is cleaned or cast here)
  * three audit columns are added: _batch_id, _source_file, _loaded_at
  * loading the same batch twice gives the same result (the batch's old rows are deleted first)
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

TABLES = ["customers", "merchants", "couriers", "orders", "order_events"]


def load_batch(batch_dir: Path, db_path: Path) -> dict[str, int]:
    batch_id = batch_dir.name
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    counts: dict[str, int] = {}
    for table in TABLES:
        csv = batch_dir / f"{table}.csv"
        if not csv.exists():
            raise FileNotFoundError(csv)
        src = f"read_csv('{csv.as_posix()}', header=true, all_varchar=true)"
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='raw' AND table_name=?", [table]
        ).fetchone()[0]
        con.execute("BEGIN")
        if not exists:
            con.execute(f"CREATE TABLE raw.{table} AS SELECT *, '' AS _batch_id, '' AS _source_file, "
                        f"now()::TIMESTAMP AS _loaded_at FROM {src} LIMIT 0")
        con.execute(f"DELETE FROM raw.{table} WHERE _batch_id = ?", [batch_id])
        con.execute(f"INSERT INTO raw.{table} SELECT *, ?, ?, now()::TIMESTAMP FROM {src}", [batch_id, csv.name])
        con.execute("COMMIT")
        counts[table] = con.execute(f"SELECT count(*) FROM raw.{table} WHERE _batch_id = ?", [batch_id]).fetchone()[0]
    con.close()
    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batch-dir", type=Path, required=True)
    p.add_argument("--db", type=Path, default=Path("warehouse/delivery.duckdb"))
    a = p.parse_args()
    for t, n in load_batch(a.batch_dir, a.db).items():
        print(f"raw.{t:14}{n:>10,} rows")


if __name__ == "__main__":
    main()
