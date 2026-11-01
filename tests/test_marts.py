"""Marts and the data-quality report, checked against manifest.json. Run the pipeline first."""
import json
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "warehouse" / "delivery.duckdb"
MANIFEST = ROOT / "data" / "raw" / "batch_001" / "manifest.json"
pytestmark = pytest.mark.skipif(not (DB.exists() and MANIFEST.exists()), reason="run the pipeline first")

EXCLUDING = ["nonpositive_subtotal_order_ids", "completed_before_placed_order_ids", "orphan_courier_order_ids",
             "null_customer_order_ids", "bad_placed_ts_order_ids"]


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(str(DB), read_only=True)
    yield c
    c.close()


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST.read_text())


def dq(con):
    return {r[0]: r[1] for r in con.execute("select check_name, rows_affected from marts.dq_report").fetchall()}


def test_dq_report_matches_manifest(con, manifest):
    d = dq(con)
    assert d["duplicate_orders"] == len(manifest["duplicate_order_ids"])
    assert d["duplicate_events"] == len(manifest["duplicate_event_ids"])
    assert d["customer_rows_superseded"] == len(manifest["changed_customer_ids"])
    assert d["status_not_standardised"] == len(set(manifest["status_variant_order_ids"]))
    assert d["nonpositive_subtotal"] == len(manifest["nonpositive_subtotal_order_ids"])
    assert d["orphan_courier"] == len(manifest["orphan_courier_order_ids"])
    assert d["null_customer"] == len(manifest["null_customer_order_ids"])
    assert d["completed_before_placed"] == len(manifest["completed_before_placed_order_ids"])
    assert d["bad_placed_ts"] == len(manifest["bad_placed_ts_order_ids"])


def test_fct_contains_exactly_the_valid_orders(con, manifest):
    excluded = set()
    for k in EXCLUDING:
        excluded |= set(manifest[k])
    in_fct = {r[0] for r in con.execute("select order_id from marts.fct_orders").fetchall()}
    assert len(in_fct) == manifest["counts"]["orders_clean"] - len(excluded)
    assert not (in_fct & excluded)


def test_gmv_formula(con):
    bad = con.execute("select count(*) from marts.fct_orders "
                      "where abs(gmv - (subtotal + delivery_fee - discount)) > 0.01").fetchone()[0]
    assert bad == 0


def test_delivery_minutes_positive_and_only_for_delivered(con):
    assert con.execute("select count(*) from marts.fct_orders where is_delivered and delivery_minutes <= 0").fetchone()[0] == 0
    assert con.execute("select count(*) from marts.fct_orders where not is_delivered and delivery_minutes is not null").fetchone()[0] == 0


def test_daily_kpis_add_up_to_fct(con):
    k = con.execute("select sum(orders_placed), sum(orders_delivered), round(sum(gmv),2) from marts.mart_daily_kpis").fetchone()
    f = con.execute("select count(*), sum(is_delivered::int), round(sum(gmv) filter (where is_delivered),2) from marts.fct_orders").fetchone()
    assert tuple(k) == tuple(f)


def test_first_order_flag_once_per_customer(con):
    n = con.execute("select count(*) from (select customer_id from marts.mart_customer_orders "
                    "where is_first_order group by 1 having count(*) <> 1)").fetchone()[0]
    assert n == 0


def test_running_gmv_ends_at_lifetime_gmv(con):
    bad = con.execute("""
        select count(*) from (
          select c.customer_id, max(o.running_gmv) as r, any_value(c.lifetime_gmv) as l
          from marts.mart_customer_orders o join marts.dim_customers c using (customer_id) group by 1
        ) where abs(r - l) > 0.01""").fetchone()[0]
    assert bad == 0


def test_rolling_average_is_within_bounds(con):
    r = con.execute("select min(deliveries_7d_avg), max(deliveries_7d_avg), min(deliveries), max(deliveries) from marts.mart_courier_daily").fetchone()
    assert r[0] >= r[2] and r[1] <= r[3]


def test_retention_is_100_percent_in_month_zero(con):
    assert con.execute("select count(*) from marts.mart_cohort_retention where months_since_first = 0 and retention <> 1").fetchone()[0] == 0
