with d as (
    select placed_date as day,
           count(*)                                                    as orders_placed,
           sum(case when is_delivered then 1 else 0 end)               as orders_delivered,
           sum(case when not is_delivered then 1 else 0 end)           as orders_cancelled,
           round(sum(case when is_delivered then gmv end), 2)              as gmv,
           round(sum(case when is_delivered then commission end), 2)       as commission,
           round(avg(delivery_minutes), 1)                             as avg_delivery_minutes,
           round({{ p90('delivery_minutes') }}, 1)              as p90_delivery_minutes,
           round(avg(case when is_delivered then cast(not is_late as {{ dbt.type_int() }}) end), 4) as on_time_rate
    from {{ ref('fct_orders') }}
    group by placed_date
)
select *,
       round(orders_cancelled * 1.0 / orders_placed, 4)                          as cancel_rate,
       round(avg(orders_placed) over (order by day rows between 6 preceding and current row), 1) as orders_7d_avg,
       round((orders_placed - lag(orders_placed, 7) over (order by day)) * 100.0
             / nullif(lag(orders_placed, 7) over (order by day), 0), 1)           as orders_wow_pct
from d
