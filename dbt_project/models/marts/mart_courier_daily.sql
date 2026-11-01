with daily as (
    select f.courier_id, c.zone_id, f.placed_date as day,   -- zone = the courier's home zone
           count(*) as deliveries,
           round(avg(f.delivery_minutes), 1) as avg_delivery_minutes,
           sum(case when f.is_late then 1 else 0 end) as late_deliveries
    from {{ ref('fct_orders') }} f
    join {{ ref('stg_couriers') }} c on c.courier_id = f.courier_id
    where f.is_delivered
    group by f.courier_id, c.zone_id, f.placed_date
)
select *,
       -- rolling 7-day average of deliveries per working day (RANGE counts calendar days, not rows)
       round(avg(deliveries) over (partition by courier_id order by {{ epoch_day('day') }}
             range between 6 preceding and current row), 2) as deliveries_7d_avg,
       dense_rank() over (partition by zone_id, day order by deliveries desc) as rank_in_zone_day
from daily
