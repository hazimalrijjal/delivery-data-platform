-- 1. rows per layer
select (select count(*) from staging.stg_orders)        as stg_orders,
       (select count(*) from staging.stg_order_events)  as stg_events,
       (select count(*) from staging.stg_customers)     as stg_customers,
       (select count(*) from marts.fct_orders)          as fct_orders;

-- 2. money and delivered orders                      
select countif(is_delivered)                                      as delivered,
       round(sum(if(is_delivered, gmv, null)), 2)                 as gmv,
       round(sum(if(is_delivered, commission, null)), 2)          as commission
from marts.fct_orders;

-- 3. customer segments                                
select segment, count(*) as n from marts.dim_customers group by segment order by segment;

-- 4. daily KPI totals                                 
select sum(orders_placed), sum(orders_delivered), round(sum(gmv), 2) from marts.mart_daily_kpis;

-- 5. window-function marts                           
select (select count(*) from marts.mart_courier_daily)   as courier_days,
       (select sum(deliveries) from marts.mart_courier_daily) as deliveries,
       (select count(*) from marts.mart_customer_orders) as customer_orders,
       (select max(order_seq) from marts.mart_customer_orders) as longest_seq;

-- 6. data-quality report: rows_affected per check
select check_name, rows_affected from marts.dq_report order by check_name;