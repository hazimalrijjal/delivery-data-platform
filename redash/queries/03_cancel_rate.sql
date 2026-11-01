-- Chart: line. X = day, Y = cancel_rate.
select day, cancel_rate, orders_cancelled, orders_placed
from marts.mart_daily_kpis
order by day
