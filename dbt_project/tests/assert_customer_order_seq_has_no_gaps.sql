-- For every customer the order sequence must be 1..N with no gaps or repeats.
select customer_id from {{ ref('mart_customer_orders') }}
group by customer_id
having max(order_seq) <> count(*) or min(order_seq) <> 1 or count(distinct order_seq) <> count(*)
