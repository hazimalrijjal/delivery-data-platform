select m.*, coalesce(s.orders_delivered, 0) as orders_delivered, round(coalesce(s.gmv, 0), 2) as gmv
from {{ ref('stg_merchants') }} m
left join (select merchant_id, count(*) as orders_delivered, sum(gmv) as gmv
           from {{ ref('fct_orders') }} where is_delivered group by merchant_id) s
  on s.merchant_id = m.merchant_id
