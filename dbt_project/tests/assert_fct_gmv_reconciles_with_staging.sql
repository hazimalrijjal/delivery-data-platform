-- Revenue in the mart must equal revenue recomputed from staging for valid delivered orders.
select * from (
    select (select sum(gmv) from {{ ref('fct_orders') }} where is_delivered) as mart_gmv,
           (select sum(round(subtotal + delivery_fee - discount, 2)) from {{ ref('stg_orders') }}
            where is_valid and status = 'delivered') as stg_gmv
) where abs(mart_gmv - stg_gmv) > 0.01
