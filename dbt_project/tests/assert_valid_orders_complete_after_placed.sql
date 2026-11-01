-- Singular test: among valid orders no delivered order may finish before it was placed. Returns offending rows.
select order_id from {{ ref('stg_orders') }}
where is_valid and status = 'delivered' and completed_ts < placed_ts
