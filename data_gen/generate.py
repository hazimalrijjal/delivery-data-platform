"""Generate synthetic delivery-platform data as CSV files, plus a manifest of every planted defect.

    python data_gen/generate.py --out data/raw/batch_001 --seed 42

Tables written (all values are plain text, like files received from a source system):
    customers, merchants, couriers, orders, order_events

The data is seeded, so the same seed always gives the same files. After the clean data is built, a fixed number of
defects are planted (duplicates, wrong casing, impossible values, orphan keys ...). Every defect is written to
manifest.json, which is the answer key: the pipeline tests compare what the pipeline finds with what was planted.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

START_DATE = "2026-07-01"
CATEGORIES = ["restaurant", "grocery", "pharmacy", "retail"]
CATEGORY_P = [0.55, 0.20, 0.10, 0.15]
PREP_MINUTES = {"restaurant": (10, 25), "grocery": (6, 14), "pharmacy": (4, 10), "retail": (5, 12)}
SUBTOTAL_MEDIAN = {"restaurant": 38.0, "grocery": 85.0, "pharmacy": 45.0, "retail": 120.0}
COMMISSION = {"restaurant": 0.20, "grocery": 0.10, "pharmacy": 0.12, "retail": 0.15}
CHANNELS = ["organic", "referral", "paid_social", "search"]
VEHICLES = ["bike", "scooter", "car"]
PAYMENTS = ["card", "cash", "wallet"]
HOUR_WEIGHTS = np.array([1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 7, 11, 14, 12, 8, 6, 6, 8, 12, 14, 13, 10, 6, 3], dtype=float)
WEEKDAY_FACTOR = {0: 0.90, 1: 0.92, 2: 0.95, 3: 1.00, 4: 1.25, 5: 1.20, 6: 1.05}   # Monday=0 ... Sunday=6

# How many defects of each kind are planted, as a share of the rows they apply to.
DEFECT_RATES = {
    "duplicate_orders": 0.010,
    "status_variants": 0.030,
    "nonpositive_subtotal": 0.004,
    "completed_before_placed": 0.003,
    "orphan_courier": 0.005,
    "null_customer": 0.002,
    "bad_placed_ts": 0.001,
    "duplicate_events": 0.005,
    "duplicate_customer_rows": 0.020,
}


def fmt_ts(series: pd.Series) -> pd.Series:
    return series.dt.strftime("%Y-%m-%d %H:%M:%S")


def make_merchants(n: int, zones: int, rng: np.random.Generator) -> pd.DataFrame:
    category = rng.choice(CATEGORIES, size=n, p=CATEGORY_P)
    onboarded = pd.to_datetime(START_DATE) - pd.to_timedelta(rng.integers(30, 900, size=n), unit="D")
    return pd.DataFrame({
        "merchant_id": [f"M{i:04d}" for i in range(1, n + 1)],
        "name": [f"{c.title()} Place {i}" for i, c in enumerate(category, start=1)],
        "category": category,
        "zone_id": rng.integers(1, zones + 1, size=n),
        "onboarded_date": onboarded.strftime("%Y-%m-%d"),
        "commission_rate": [COMMISSION[c] for c in category],
    })


def make_couriers(n: int, zones: int, rng: np.random.Generator) -> pd.DataFrame:
    hired = pd.to_datetime(START_DATE) - pd.to_timedelta(rng.integers(10, 700, size=n), unit="D")
    return pd.DataFrame({
        "courier_id": [f"R{i:04d}" for i in range(1, n + 1)],
        "hire_date": hired.strftime("%Y-%m-%d"),
        "vehicle_type": rng.choice(VEHICLES, size=n, p=[0.35, 0.45, 0.20]),
        "zone_id": rng.integers(1, zones + 1, size=n),
        "status": rng.choice(["active", "inactive"], size=n, p=[0.92, 0.08]),
    })


def make_customers(n: int, zones: int, days: int, rng: np.random.Generator) -> pd.DataFrame:
    start = pd.to_datetime(START_DATE)
    early = rng.random(n) < 0.80                                   # 80% signed up before the window, 20% during it
    offset = np.where(early, -rng.integers(1, 365, size=n), rng.integers(0, days, size=n))
    signup = start + pd.to_timedelta(offset, unit="D")
    return pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(1, n + 1)],
        "signup_date": signup.strftime("%Y-%m-%d"),
        "zone_id": rng.integers(1, zones + 1, size=n),
        "acquisition_channel": rng.choice(CHANNELS, size=n, p=[0.40, 0.20, 0.25, 0.15]),
        "updated_at": signup.strftime("%Y-%m-%d") + " 00:00:00",
    })


def make_orders(customers, merchants, couriers, days: int, orders_per_day: int, rng: np.random.Generator):
    start = pd.to_datetime(START_DATE)
    day_index = np.arange(days)
    dates = start + pd.to_timedelta(day_index, unit="D")
    growth = 1 + 0.004 * day_index                                   # slow growth over the window
    lam = orders_per_day * growth * np.array([WEEKDAY_FACTOR[d.weekday()] for d in dates])
    per_day = rng.poisson(lam)
    n = int(per_day.sum())
    day_of_order = np.repeat(day_index, per_day)
    hour = rng.choice(24, size=n, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum())
    placed = (start + pd.to_timedelta(day_of_order, unit="D") + pd.to_timedelta(hour, unit="h")
              + pd.to_timedelta(rng.integers(0, 3600, size=n), unit="s"))
    placed = pd.Series(placed)

    # customers: heavy users order more (gamma weights); a customer cannot order before signing up
    weights = rng.gamma(shape=0.8, scale=1.0, size=len(customers))
    weights = weights / weights.sum()
    signup = pd.to_datetime(customers["signup_date"]).to_numpy()
    cust_idx = rng.choice(len(customers), size=n, p=weights)
    for _ in range(30):
        bad = signup[cust_idx] > placed.to_numpy()
        if not bad.any():
            break
        cust_idx[bad] = rng.choice(len(customers), size=int(bad.sum()), p=weights)
    bad = signup[cust_idx] > placed.to_numpy()
    cust_idx[bad] = int(np.argmin(signup))                           # fallback: the earliest customer
    cust_zone = customers["zone_id"].to_numpy()[cust_idx]

    # merchants: 85% of orders from a merchant in the customer's zone, popular merchants get more orders
    popularity = rng.zipf(1.6, size=len(merchants)).astype(float).clip(max=60)
    m_zone = merchants["zone_id"].to_numpy()
    m_idx = np.full(n, -1, dtype=int)                                # -1 = not chosen yet
    local = rng.random(n) < 0.85
    for z in np.unique(cust_zone):
        in_zone = np.where(m_zone == z)[0]
        sel = np.where(cust_zone == z)[0]
        if len(in_zone) == 0:
            local[sel] = False
            continue
        p = popularity[in_zone] / popularity[in_zone].sum()
        pick = rng.choice(in_zone, size=len(sel), p=p)
        m_idx[sel] = np.where(local[sel], pick, -1)
    anywhere = np.where(m_idx == -1)[0]
    m_idx[anywhere] = rng.choice(len(merchants), size=len(anywhere), p=popularity / popularity.sum())
    category = merchants["category"].to_numpy()[m_idx]

    status = rng.choice(["delivered", "cancelled_customer", "cancelled_merchant", "cancelled_no_courier"],
                        size=n, p=[0.925, 0.030, 0.025, 0.020])
    delivered = status == "delivered"

    # couriers: someone active from the merchant's zone, none if nobody was found
    active = couriers[couriers["status"] == "active"]
    courier_id = np.full(n, None, dtype=object)
    for z in np.unique(m_zone[m_idx]):
        pool = active[active["zone_id"] == z]["courier_id"].to_numpy()
        sel = np.where((m_zone[m_idx] == z) & (status != "cancelled_no_courier"))[0]
        if len(pool) == 0:
            pool = active["courier_id"].to_numpy()
        courier_id[sel] = rng.choice(pool, size=len(sel))

    # amounts
    median = np.array([SUBTOTAL_MEDIAN[c] for c in category])
    subtotal = np.round(median * rng.lognormal(0, 0.45, size=n), 2).clip(min=5)
    delivery_fee = np.round(rng.choice([3.0, 5.0, 7.0, 9.0, 12.0], size=n, p=[0.15, 0.35, 0.25, 0.15, 0.10]), 2)
    discount = np.where(rng.random(n) < 0.18, np.round(subtotal * rng.uniform(0.05, 0.30, size=n), 2), 0.0)

    # times: accepted, picked up, delivered (rush hours are slower)
    rush = np.isin(hour, [12, 13, 19, 20, 21])
    accept_s = rng.gamma(2.0, 40.0, size=n) + 20
    prep_lo = np.array([PREP_MINUTES[c][0] for c in category])
    prep_hi = np.array([PREP_MINUTES[c][1] for c in category])
    prep_s = rng.uniform(prep_lo, prep_hi) * 60 * np.where(rush, 1.25, 1.0)
    travel_s = rng.gamma(4.0, 3.2, size=n) * 60 * np.where(rush, 1.2, 1.0) + 240
    accepted = placed + pd.to_timedelta(accept_s, unit="s")
    picked = accepted + pd.to_timedelta(prep_s, unit="s")
    done = picked + pd.to_timedelta(travel_s, unit="s")
    cancel_at = placed + pd.to_timedelta(rng.uniform(60, 900, size=n), unit="s")
    completed = pd.Series(np.where(delivered, done, cancel_at))
    completed = pd.to_datetime(completed)

    orders = pd.DataFrame({
        "order_id": [f"O{i:08d}" for i in range(1, n + 1)],
        "customer_id": customers["customer_id"].to_numpy()[cust_idx],
        "merchant_id": merchants["merchant_id"].to_numpy()[m_idx],
        "courier_id": courier_id,
        "zone_id": cust_zone,
        "placed_ts": placed,
        "completed_ts": completed,
        "status": status,
        "subtotal": subtotal,
        "delivery_fee": delivery_fee,
        "discount": discount,
        "payment_method": rng.choice(PAYMENTS, size=n, p=[0.62, 0.18, 0.20]),
        "promised_minutes": np.where(category == "restaurant", 45, 55),
    })

    ev = []
    base = pd.DataFrame({"order_id": orders["order_id"], "event_type": "placed", "event_ts": placed})
    ev.append(base)
    ev.append(pd.DataFrame({"order_id": orders["order_id"][~(status == "cancelled_no_courier")],
                            "event_type": "accepted", "event_ts": accepted[~(status == "cancelled_no_courier")]}))
    ev.append(pd.DataFrame({"order_id": orders["order_id"][delivered], "event_type": "picked_up", "event_ts": picked[delivered]}))
    ev.append(pd.DataFrame({"order_id": orders["order_id"][delivered], "event_type": "delivered", "event_ts": done[delivered]}))
    ev.append(pd.DataFrame({"order_id": orders["order_id"][~delivered], "event_type": "cancelled", "event_ts": cancel_at[~delivered]}))
    events = pd.concat(ev, ignore_index=True).sort_values(["event_ts", "order_id"]).reset_index(drop=True)
    events.insert(0, "event_id", [f"E{i:09d}" for i in range(1, len(events) + 1)])
    return orders, events


def plant_defects(customers, orders, events, couriers, rng: np.random.Generator):
    """Turn the clean tables into 'as received' text tables and return them with the manifest (the answer key)."""
    manifest: dict = {"rates": DEFECT_RATES}
    o = orders.copy()
    o["placed_ts"] = fmt_ts(pd.to_datetime(o["placed_ts"]))
    o["completed_ts"] = fmt_ts(pd.to_datetime(o["completed_ts"]))
    o = o.astype(object)
    available = set(o.index)

    def take(rate: float, pool=None) -> np.ndarray:
        pool_idx = np.array(sorted(available if pool is None else set(pool) & available))
        k = int(round(rate * len(o)))
        chosen = rng.choice(pool_idx, size=min(k, len(pool_idx)), replace=False)
        available.difference_update(chosen.tolist())
        return np.sort(chosen)

    idx = take(DEFECT_RATES["nonpositive_subtotal"])
    o.loc[idx, "subtotal"] = rng.choice(["-12.50", "0.00", "-3.00"], size=len(idx))
    manifest["nonpositive_subtotal_order_ids"] = o.loc[idx, "order_id"].tolist()

    delivered_idx = o.index[o["status"] == "delivered"]
    idx = take(DEFECT_RATES["completed_before_placed"], pool=delivered_idx)
    for i in idx:
        placed = pd.to_datetime(o.at[i, "placed_ts"])
        o.at[i, "completed_ts"] = (placed - pd.Timedelta(minutes=int(rng.integers(5, 60)))).strftime("%Y-%m-%d %H:%M:%S")
    manifest["completed_before_placed_order_ids"] = o.loc[idx, "order_id"].tolist()

    with_courier = o.index[o["courier_id"].notna()]
    idx = take(DEFECT_RATES["orphan_courier"], pool=with_courier)
    o.loc[idx, "courier_id"] = [f"R9{int(x):03d}" for x in rng.integers(0, 999, size=len(idx))]
    manifest["orphan_courier_order_ids"] = o.loc[idx, "order_id"].tolist()

    idx = take(DEFECT_RATES["null_customer"])
    o.loc[idx, "customer_id"] = None
    manifest["null_customer_order_ids"] = o.loc[idx, "order_id"].tolist()

    idx = take(DEFECT_RATES["bad_placed_ts"])
    o.loc[idx, "placed_ts"] = "not_a_date"
    manifest["bad_placed_ts_order_ids"] = o.loc[idx, "order_id"].tolist()

    idx = take(DEFECT_RATES["status_variants"])
    variants = []
    for s in o.loc[idx, "status"]:
        variants.append(rng.choice([s.upper(), s.title(), f" {s}", f"{s} "]))
    o.loc[idx, "status"] = variants
    manifest["status_variant_order_ids"] = o.loc[idx, "order_id"].tolist()

    dup_idx = rng.choice(o.index.to_numpy(), size=int(round(DEFECT_RATES["duplicate_orders"] * len(o))), replace=False)
    manifest["duplicate_order_ids"] = sorted(o.loc[dup_idx, "order_id"].tolist())
    o = pd.concat([o, o.loc[dup_idx]], ignore_index=True)

    e = events.copy()
    e["event_ts"] = fmt_ts(pd.to_datetime(e["event_ts"]))
    e = e.astype(object)
    dup_e = rng.choice(e.index.to_numpy(), size=int(round(DEFECT_RATES["duplicate_events"] * len(e))), replace=False)
    manifest["duplicate_event_ids"] = sorted(e.loc[dup_e, "event_id"].tolist())
    e = pd.concat([e, e.loc[dup_e]], ignore_index=True)

    c = customers.copy().astype(object)
    dup_c = rng.choice(c.index.to_numpy(), size=int(round(DEFECT_RATES["duplicate_customer_rows"] * len(c))), replace=False)
    changed = c.loc[dup_c].copy()
    changed["zone_id"] = (changed["zone_id"].astype(int) % 12) + 1             # the customer moved to another zone
    changed["updated_at"] = (pd.to_datetime(changed["signup_date"]) + pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d") + " 12:00:00"   # the new row is always the latest
    manifest["changed_customer_ids"] = sorted(changed["customer_id"].tolist())
    manifest["changed_customer_new_zone"] = {r.customer_id: int(r.zone_id) for r in changed.itertuples()}
    c = pd.concat([c, changed], ignore_index=True)

    manifest["counts"] = {
        "customers_clean": len(customers), "orders_clean": len(orders), "events_clean": len(events),
        "orders_in_file": len(o), "events_in_file": len(e), "customers_in_file": len(c),
    }
    return c, o, e, manifest


def generate(out: Path, seed: int = 42, customers: int = 5000, merchants: int = 300, couriers: int = 400,
             days: int = 90, orders_per_day: int = 700, zones: int = 12) -> dict:
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)
    m = make_merchants(merchants, zones, rng)
    r = make_couriers(couriers, zones, rng)
    c = make_customers(customers, zones, days, rng)
    o, ev = make_orders(c, m, r, days, orders_per_day, rng)
    c_file, o_file, e_file, manifest = plant_defects(c, o, ev, r, rng)
    manifest["seed"] = seed
    manifest["start_date"] = START_DATE
    manifest["days"] = days
    for name, df in [("customers", c_file), ("merchants", m), ("couriers", r), ("orders", o_file), ("order_events", e_file)]:
        df.to_csv(out / f"{name}.csv", index=False)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    return manifest


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--customers", type=int, default=5000)
    p.add_argument("--merchants", type=int, default=300)
    p.add_argument("--couriers", type=int, default=400)
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--orders-per-day", type=int, default=700)
    a = p.parse_args()
    manifest = generate(a.out, a.seed, a.customers, a.merchants, a.couriers, a.days, a.orders_per_day)
    for k, v in manifest["counts"].items():
        print(f"{k:20}{v:>10,}")
    print(f"written to {a.out}")


if __name__ == "__main__":
    main()
