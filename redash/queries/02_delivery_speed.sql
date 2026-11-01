-- Chart: line. X = day, Y = avg_delivery_minutes and p90_delivery_minutes.
select day, avg_delivery_minutes, p90_delivery_minutes, on_time_rate
from marts.mart_daily_kpis
where avg_delivery_minutes is not null
order by day
