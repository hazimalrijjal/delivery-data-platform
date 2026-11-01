-- Chart: bar (horizontal). Category = merchant_name, value = gmv.
select merchant_name, category, orders_delivered, gmv
from marts.dim_merchants
order by gmv desc
limit 10
