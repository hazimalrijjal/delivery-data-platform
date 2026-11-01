with stats as (
    select customer_id,
           min(placed_date)  as first_order_date,
           max(placed_date)  as last_order_date,
           count(*)          as lifetime_orders,
           sum(gmv)          as lifetime_gmv
    from {{ ref('fct_orders') }}
    where is_delivered
    group by customer_id
)
select
    c.customer_id, c.signup_date, c.zone_id, c.acquisition_channel,
    s.first_order_date, s.last_order_date,
    coalesce(s.lifetime_orders, 0)        as lifetime_orders,
    round(coalesce(s.lifetime_gmv, 0), 2) as lifetime_gmv,
    case when s.lifetime_orders is null then 'never_ordered'
         when s.lifetime_orders = 1 then 'one_time'
         when s.lifetime_orders < 8 then 'repeat'
         else 'loyal' end                 as segment
from {{ ref('stg_customers') }} c
left join stats s on s.customer_id = c.customer_id
