-- Monthly cohorts by first delivered order; retention = share of the cohort that ordered again N months later.
with first_order as (
    select customer_id, {{ dbt.date_trunc('month', 'min(placed_date)') }} as cohort_month
    from {{ ref('fct_orders') }} where is_delivered and customer_id is not null group by customer_id
),
activity as (
    select distinct customer_id, {{ dbt.date_trunc('month', 'placed_date') }} as active_month
    from {{ ref('fct_orders') }} where is_delivered and customer_id is not null
),
sizes as (select cohort_month, count(*) as cohort_size from first_order group by cohort_month)
select f.cohort_month,
       {{ dbt.datediff('f.cohort_month', 'a.active_month', 'month') }} as months_since_first,
       count(distinct a.customer_id)                      as active_customers,
       s.cohort_size,
       round(count(distinct a.customer_id) * 1.0 / s.cohort_size, 4) as retention
from first_order f
join activity a using (customer_id)
join sizes s on s.cohort_month = f.cohort_month
group by f.cohort_month, a.active_month, s.cohort_size
