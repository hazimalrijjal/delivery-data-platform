-- Chart: line. X = day, Y = orders_placed and orders_7d_avg (GMV on a second axis).
select day, orders_placed, orders_7d_avg, gmv
from marts.mart_daily_kpis
order by day
