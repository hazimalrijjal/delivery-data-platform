{{ config(materialized=fct_orders_materialization(), unique_key='order_id', incremental_strategy=incremental_strategy_for_target()) }}
-- One row per VALID order (orders with a data-quality flag are excluded here and counted in dq_report).
-- Incremental: on later runs only orders from (latest placed_ts - 1 day) onwards are rebuilt. The 1 day overlap
-- catches late-arriving rows, and delete+insert on order_id means a re-processed order replaces its old row.
with orders as (
    select * from {{ ref('stg_orders') }}
    where is_valid
    {% if is_incremental() %}
      and placed_ts >= {{ ts_minus_days('(select max(placed_ts) from ' ~ this ~ ')', 1) }}
    {% endif %}
),
event_times as (
    select order_id,
           min(case when event_type = 'accepted'  then event_ts end) as accepted_ts,
           min(case when event_type = 'picked_up' then event_ts end) as picked_up_ts
    from {{ ref('stg_order_events') }}
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    o.merchant_id,
    o.courier_id,
    o.zone_id,
    m.category                                                        as merchant_category,
    o.placed_ts,
    cast(o.placed_ts as date)                                         as placed_date,
    extract(hour from o.placed_ts)                                    as placed_hour,
    e.accepted_ts,
    e.picked_up_ts,
    o.completed_ts,
    o.status,
    (o.status = 'delivered')                                          as is_delivered,
    o.payment_method,
    o.subtotal,
    o.delivery_fee,
    o.discount,
    round(o.subtotal + o.delivery_fee - o.discount, 2)                as gmv,
    round(o.subtotal * m.commission_rate, 2)                          as commission,
    o.promised_minutes,
    case when o.status = 'delivered'
         then cast(round({{ dbt.datediff('o.placed_ts', 'o.completed_ts', 'second') }} / 60.0, 1) as {{ dbt.type_numeric() }}) end   as delivery_minutes,   -- exact decimal: averages of it do not depend on summation order
    case when o.status = 'delivered'
         then {{ dbt.datediff('o.placed_ts', 'o.completed_ts', 'second') }} / 60.0 > o.promised_minutes end as is_late
from orders o
join {{ ref('stg_merchants') }} m on m.merchant_id = o.merchant_id
left join event_times e on e.order_id = o.order_id
