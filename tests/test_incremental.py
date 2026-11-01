"""Proves fct_orders is truly incremental: delete the latest days, run dbt WITHOUT --full-refresh, rows come back,
and an untouched older row is not rebuilt. Needs the dbt command on PATH (venv active)."""
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "warehouse" / "delivery.duckdb"
pytestmark = pytest.mark.skipif(not DB.exists() or shutil.which("dbt") is None, reason="needs pipeline + dbt on PATH")


def dbt(*args):
    r = subprocess.run(["dbt", *args, "--profiles-dir", "."], cwd=ROOT / "dbt_project", capture_output=True, text=True)
    assert r.returncode == 0, r.stdout[-2000:]


def test_incremental_run_restores_deleted_days_and_leaves_old_rows_alone():
    con = duckdb.connect(str(DB))
    total = con.execute("select count(*) from marts.fct_orders").fetchone()[0]
    last_day = con.execute("select max(placed_date) from marts.fct_orders").fetchone()[0]
    # tamper: delete the last 3 days, and change one OLD row so we can tell whether it gets rebuilt
    con.execute("delete from marts.fct_orders where placed_date >= ?::date - interval 2 day", [last_day])
    old = con.execute("select order_id from marts.fct_orders order by placed_ts limit 1").fetchone()[0]
    con.execute("update marts.fct_orders set gmv = -1 where order_id = ?", [old])
    con.close()
    try:
        dbt("run", "--select", "fct_orders")           # incremental: no --full-refresh
        con = duckdb.connect(str(DB), read_only=True)
        after = con.execute("select count(*) from marts.fct_orders").fetchone()[0]
        old_gmv = con.execute("select gmv from marts.fct_orders where order_id = ?", [old]).fetchone()[0]
        con.close()
        # the watermark is the table's own latest placed_ts, so the deleted days are picked up again (self-healing) ...
        assert after == total
        # ... while the tampered OLD row was not rebuilt, which proves the run was incremental, not a full rebuild
        assert old_gmv == -1
    finally:
        dbt("run", "--select", "fct_orders", "--full-refresh")   # always leave the warehouse correct for the other tests
    con = duckdb.connect(str(DB), read_only=True)
    assert con.execute("select count(*) from marts.fct_orders where gmv = -1").fetchone()[0] == 0
    con.close()
