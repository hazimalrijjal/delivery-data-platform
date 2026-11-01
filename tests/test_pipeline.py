"""Checks the pipeline against manifest.json (the list of planted defects).

Run the pipeline first (scripts/run_week1.bat or the commands in README.md), then:  pytest -q
"""
import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "warehouse" / "delivery.duckdb"
MANIFEST = ROOT / "data" / "raw" / "batch_001" / "manifest.json"

needs_pipeline = pytest.mark.skipif(not (DB.exists() and MANIFEST.exists()), reason="run the pipeline first")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(str(DB), read_only=True)
    yield c
    c.close()


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST.read_text())


def flagged(con, column):
    return {r[0] for r in con.execute(f"select order_id from staging.stg_orders where {column}").fetchall()}


def test_generator_is_deterministic(tmp_path):
    sys.path.insert(0, str(ROOT / "data_gen"))
    import generate
    a = generate.generate(tmp_path / "a", seed=7, customers=200, merchants=20, couriers=30, days=5, orders_per_day=50)
    b = generate.generate(tmp_path / "b", seed=7, customers=200, merchants=20, couriers=30, days=5, orders_per_day=50)
    assert a == b
    assert (tmp_path / "a" / "orders.csv").read_text() == (tmp_path / "b" / "orders.csv").read_text()


@needs_pipeline
def test_row_counts_after_dedupe(con, manifest):
    c = manifest["counts"]
    assert con.execute("select count(*) from staging.stg_orders").fetchone()[0] == c["orders_clean"]
    assert con.execute("select count(*) from staging.stg_order_events").fetchone()[0] == c["events_clean"]
    assert con.execute("select count(*) from staging.stg_customers").fetchone()[0] == c["customers_clean"]


@needs_pipeline
@pytest.mark.parametrize("key,column", [
    ("nonpositive_subtotal_order_ids", "flag_nonpositive_subtotal"),
    ("completed_before_placed_order_ids", "flag_completed_before_placed"),
    ("orphan_courier_order_ids", "flag_orphan_courier"),
    ("null_customer_order_ids", "flag_null_customer"),
    ("bad_placed_ts_order_ids", "flag_bad_placed_ts"),
])
def test_every_planted_defect_is_flagged_exactly(con, manifest, key, column):
    assert flagged(con, column) == set(manifest[key])


@needs_pipeline
def test_status_variants_are_standardised(con):
    bad = con.execute("select count(*) from staging.stg_orders "
                      "where status not in ('delivered','cancelled_customer','cancelled_merchant','cancelled_no_courier')").fetchone()[0]
    assert bad == 0


@needs_pipeline
def test_customer_keeps_latest_zone(con, manifest):
    for cid, zone in list(manifest["changed_customer_new_zone"].items())[:50]:
        assert con.execute("select zone_id from staging.stg_customers where customer_id=?", [cid]).fetchone()[0] == zone


@needs_pipeline
def test_valid_flag_matches_flags(con, manifest):
    planted = set()
    for k in ("nonpositive_subtotal_order_ids", "completed_before_placed_order_ids", "orphan_courier_order_ids",
              "null_customer_order_ids", "bad_placed_ts_order_ids"):
        planted |= set(manifest[k])
    invalid = {r[0] for r in con.execute("select order_id from staging.stg_orders where not is_valid").fetchall()}
    assert invalid == planted


@needs_pipeline
def test_reloading_a_batch_does_not_duplicate(con):
    n = con.execute("select count(*) from raw.orders").fetchone()[0]
    d = con.execute("select count(distinct _batch_id) from raw.orders").fetchone()[0]
    assert d == 1 and n > 0
