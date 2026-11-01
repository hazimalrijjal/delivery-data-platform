-- One row per event. The source delivers at-least-once, so the same event_id can arrive twice.
with ranked as (
    select *,
           row_number() over (partition by event_id order by _loaded_at desc) as rn
    from {{ source('raw', 'order_events') }}
)
select
    event_id,
    order_id,
    event_type,
    cast(event_ts as timestamp) as event_ts
from ranked
where rn = 1
