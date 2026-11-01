"""Reconcile the PySpark batch job against the dbt mart (mart_daily_kpis in DuckDB), day by day.

Skipped when pyspark or Java is missing, or when the pipeline has not been run (see README).
"""
import shutil
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "warehouse" / "delivery.duckdb"
BATCH = ROOT / "data" / "raw" / "batch_001"
pytestmark = pytest.mark.skipif(
    shutil.which("java") is None or not DB.exists() or not BATCH.exists(),
    reason="needs Java, the DuckDB warehouse and data/raw/batch_001",
)
sys.path.insert(0, str(ROOT))

# Everything must match exactly. This was not always so: with money stored as floating point, 96 orders differed by one cent
# (e.g. 64.85 * 0.1 is 6.484999999999999 as a double but exactly 6.485 as a decimal). Money is a decimal in both engines now.
EXACT = ["orders_placed", "orders_delivered", "orders_cancelled"]
TOLERANCE = {"gmv": 1e-6, "commission": 1e-6, "avg_delivery_minutes": 1e-6, "p90_delivery_minutes": 1e-6,
             "on_time_rate": 1e-6, "cancel_rate": 1e-6, "orders_7d_avg": 1e-6, "orders_wow_pct": 1e-6}


@pytest.fixture(scope="module")
def frames():
    from spark_jobs.batch_daily_kpis import run
    from spark_jobs.session import get_spark
    spark = get_spark("test_batch")
    spark_df = run(spark, BATCH).toPandas()
    con = duckdb.connect(str(DB), read_only=True)
    dbt_df = con.execute("select * from marts.mart_daily_kpis order by day").df()
    con.close()
    spark.stop()
    for df in (spark_df, dbt_df):
        df["day"] = pd.to_datetime(df["day"])            # Spark returns dates, DuckDB timestamps: compare like with like
    return spark_df.set_index("day").sort_index(), dbt_df.set_index("day").sort_index()


def test_same_days(frames):
    s, d = frames
    assert list(s.index) == list(d.index) and len(s) == 90


@pytest.mark.parametrize("col", EXACT)
def test_counts_are_identical(frames, col):
    s, d = frames
    assert (s[col].astype(int) == d[col].astype(int)).all()


@pytest.mark.parametrize("col,tol", TOLERANCE.items())
def test_values_agree_exactly(frames, col, tol):
    s, d = frames
    diff = (s[col].astype(float) - d[col].astype(float)).abs().fillna(0)
    assert diff.max() <= tol, f"{col}: largest difference {diff.max()} on {diff.idxmax()}"
    nan_s, nan_d = s[col].isna(), d[col].isna()
    assert (nan_s == nan_d).all()                        # NULL where dbt has NULL (the first 7 days have no week-over-week)


def test_totals_match(frames):
    s, d = frames
    assert s["orders_placed"].sum() == d["orders_placed"].sum() == 76321
    assert round(float(s["gmv"].sum()), 2) == round(float(d["gmv"].sum()), 2)
    assert round(float(s["commission"].sum()), 2) == round(float(d["commission"].sum()), 2) == 709496.31
