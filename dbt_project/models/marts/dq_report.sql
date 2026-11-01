-- One row per data-quality check: what was found in the data as received. Row counts come from raw vs staging,
-- flags come from stg_orders. severity: 'fixed' = the pipeline repaired it, 'excluded' = the row is kept out of fct_orders.
with o as (select * from {{ ref('stg_orders') }}),
raw_orders as (select count(*) as n, count(distinct order_id) as d from {{ source('raw', 'orders') }}),
raw_events as (select count(*) as n, count(distinct event_id) as d from {{ source('raw', 'order_events') }}),
raw_customers as (select count(*) as n, count(distinct customer_id) as d from {{ source('raw', 'customers') }}),
checks as (
    select 'duplicate_orders' as check_name, 'fixed' as severity, n - d as rows_affected, n as rows_checked from raw_orders
    union all select 'duplicate_events', 'fixed', n - d, n from raw_events
    union all select 'customer_rows_superseded', 'fixed', n - d, n from raw_customers
    union all select 'status_not_standardised', 'fixed',
        (select count(distinct order_id) from {{ source('raw', 'orders') }} where status <> lower(trim(status))),
        (select count(*) from o)
    union all select 'bad_placed_ts',               'excluded', sum(case when flag_bad_placed_ts then 1 else 0 end),           count(*) from o
    union all select 'nonpositive_subtotal',        'excluded', sum(case when flag_nonpositive_subtotal then 1 else 0 end),    count(*) from o
    union all select 'null_customer',               'excluded', sum(case when flag_null_customer then 1 else 0 end),           count(*) from o
    union all select 'orphan_courier',              'excluded', sum(case when flag_orphan_courier then 1 else 0 end),          count(*) from o
    union all select 'completed_before_placed',     'excluded', sum(case when flag_completed_before_placed then 1 else 0 end), count(*) from o
    union all select 'orders_excluded_from_fct',    'excluded', sum(case when not is_valid then 1 else 0 end),                 count(*) from o
)
select check_name, severity, rows_affected, rows_checked,
       round(rows_affected * 100.0 / rows_checked, 3) as pct_affected
from checks
