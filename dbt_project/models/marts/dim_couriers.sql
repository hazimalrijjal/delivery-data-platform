select c.*, coalesce(s.deliveries, 0) as deliveries, s.avg_delivery_minutes
from {{ ref('stg_couriers') }} c
left join (select courier_id, count(*) as deliveries, round(avg(delivery_minutes), 1) as avg_delivery_minutes
           from {{ ref('fct_orders') }} where is_delivered group by courier_id) s
  on s.courier_id = c.courier_id
