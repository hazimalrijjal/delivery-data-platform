"""Plain-Python steps of the daily pipeline. The Airflow DAG only calls these, so they can be tested without Airflow.

Each function either returns normally or raises an exception (which is how Airflow knows a task failed).
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingest"))

REQUIRED_FILES = ["customers.csv", "merchants.csv", "couriers.csv", "orders.csv", "order_events.csv"]

# Quality gate: the pipeline stops (and the dashboard data is not refreshed) when a check goes past its limit.
# 'excluded' checks are rows kept out of fct_orders; 'fixed' checks are rows the pipeline repaired.
MAX_PCT = {"excluded": 3.0, "fixed": 10.0}
EXPECTED_CHECKS = {
    "duplicate_orders", "duplicate_events", "customer_rows_superseded", "status_not_standardised",
    "bad_placed_ts", "nonpositive_subtotal", "null_customer", "orphan_courier", "completed_before_placed",
    "orders_excluded_from_fct",
}


class DataQualityError(Exception):
    pass


def batch_ready(batch_dir: str | Path) -> bool:
    """True when every expected file of the batch exists and is not empty (used by the sensor)."""
    d = Path(batch_dir)
    return all((d / f).exists() and (d / f).stat().st_size > 0 for f in REQUIRED_FILES)


def load_batch_to_raw(batch_dir: str | Path, db_path: str | Path) -> dict[str, int]:
    """Load the batch into the raw layer. Safe to run again for the same batch (idempotent)."""
    from load_raw import load_batch
    if not batch_ready(batch_dir):
        raise FileNotFoundError(f"batch is incomplete: {batch_dir}")
    return load_batch(Path(batch_dir), Path(db_path))


def check_data_quality(db_path: str | Path, max_pct: dict[str, float] | None = None) -> list[str]:
    """Read marts.dq_report and raise DataQualityError if a check is missing or over its limit. Returns a summary."""
    limits = {**MAX_PCT, **(max_pct or {})}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute("select check_name, severity, rows_affected, rows_checked, pct_affected "
                           "from marts.dq_report").fetchall()
    finally:
        con.close()
    found = {r[0] for r in rows}
    if found != EXPECTED_CHECKS:
        raise DataQualityError(f"dq_report checks differ from expected: missing={sorted(EXPECTED_CHECKS - found)} "
                               f"unexpected={sorted(found - EXPECTED_CHECKS)}")
    problems, summary = [], []
    for name, severity, affected, checked, pct in rows:
        summary.append(f"{name}: {affected}/{checked} ({pct}%) [{severity}]")
        if checked == 0:
            problems.append(f"{name}: no rows were checked")
        elif pct > limits[severity]:
            problems.append(f"{name}: {pct}% is over the {limits[severity]}% limit")
    if problems:
        raise DataQualityError("; ".join(problems))
    return summary
