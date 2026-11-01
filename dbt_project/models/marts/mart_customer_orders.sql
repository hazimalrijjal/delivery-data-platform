-- Window functions per customer, over delivered orders in time order.
select
    order_id,
    customer_id,
    placed_ts,
    gmv,
    row_number() over w                                                   as order_seq,          -- 1 = first order
    row_number() over w = 1                                               as is_first_order,
    lag(placed_ts) over w                                                 as prev_order_ts,
    round({{ dbt.datediff('(lag(placed_ts) over w)', 'placed_ts', 'minute') }} / 1440.0, 1) as days_since_prev,
    round(sum(gmv) over (partition by customer_id order by placed_ts, order_id
                         rows between unbounded preceding and current row), 2) as running_gmv
from {{ ref('fct_orders') }}
where is_delivered and customer_id is not null
window w as (partition by customer_id order by placed_ts, order_id)
