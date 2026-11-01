select courier_id, day from {{ ref('mart_courier_daily') }}
group by courier_id, day having count(*) > 1
