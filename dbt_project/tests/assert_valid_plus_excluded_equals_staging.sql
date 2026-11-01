-- Nothing may disappear: orders in fct_orders + excluded orders = orders in staging.
select * from (
    select (select count(*) from {{ ref('fct_orders') }}) + (select count(*) from {{ ref('stg_orders') }} where not is_valid) as a,
           (select count(*) from {{ ref('stg_orders') }}) as b
) where a <> b
