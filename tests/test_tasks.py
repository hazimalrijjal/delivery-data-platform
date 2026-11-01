"""The pipeline task functions (no Airflow needed)."""
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline import tasks  # noqa: E402

DB = ROOT / "warehouse" / "delivery.duckdb"


def test_batch_ready_false_for_missing_and_empty(tmp_path):
    assert not tasks.batch_ready(tmp_path)
    for f in tasks.REQUIRED_FILES:
        (tmp_path / f).write_text("")
    assert not tasks.batch_ready(tmp_path)           # files exist but are empty
    for f in tasks.REQUIRED_FILES:
        (tmp_path / f).write_text("a\n1\n")
    assert tasks.batch_ready(tmp_path)


def fake_db(path, overrides=None, drop=None):
    con = duckdb.connect(str(path))
    con.execute("create schema marts")
    con.execute("create table marts.dq_report(check_name varchar, severity varchar, rows_affected bigint, "
                "rows_checked bigint, pct_affected double)")
    for name in sorted(tasks.EXPECTED_CHECKS):
        if name == drop:
            continue
        sev = "fixed" if name in ("duplicate_orders", "duplicate_events", "customer_rows_superseded",
                                  "status_not_standardised") else "excluded"
        pct = (overrides or {}).get(name, 0.5)
        con.execute("insert into marts.dq_report values (?,?,?,?,?)", [name, sev, int(pct * 10), 1000, pct])
    con.close()
    return path


def test_gate_passes_when_all_checks_are_within_limits(tmp_path):
    out = tasks.check_data_quality(fake_db(tmp_path / "a.duckdb"))
    assert len(out) == len(tasks.EXPECTED_CHECKS)


def test_gate_fails_when_a_check_is_over_its_limit(tmp_path):
    with pytest.raises(tasks.DataQualityError, match="null_customer"):
        tasks.check_data_quality(fake_db(tmp_path / "b.duckdb", {"null_customer": 9.0}))


def test_gate_fails_when_a_check_is_missing(tmp_path):
    with pytest.raises(tasks.DataQualityError, match="missing"):
        tasks.check_data_quality(fake_db(tmp_path / "c.duckdb", drop="orphan_courier"))


def test_gate_limit_depends_on_severity(tmp_path):
    # 5% is fine for a 'fixed' check (limit 10) but too much for an 'excluded' one (limit 3)
    tasks.check_data_quality(fake_db(tmp_path / "d.duckdb", {"duplicate_orders": 5.0}))
    with pytest.raises(tasks.DataQualityError):
        tasks.check_data_quality(fake_db(tmp_path / "e.duckdb", {"orphan_courier": 5.0}))


@pytest.mark.skipif(not DB.exists(), reason="run the pipeline first")
def test_gate_passes_on_the_real_warehouse():
    tasks.check_data_quality(DB)


def test_incomplete_batch_is_refused(tmp_path):
    with pytest.raises(FileNotFoundError):
        tasks.load_batch_to_raw(tmp_path, tmp_path / "x.duckdb")
