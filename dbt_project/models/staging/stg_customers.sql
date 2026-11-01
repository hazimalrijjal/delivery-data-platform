-- One row per customer. The source can send a customer twice (e.g. after a change of zone); keep the latest row.
with ranked as (
    select *,
           row_number() over (partition by customer_id order by updated_at desc, _loaded_at desc) as rn
    from {{ source('raw', 'customers') }}
)
select
    customer_id,
    cast(signup_date as date)        as signup_date,
    cast(zone_id as integer)         as zone_id,
    acquisition_channel,
    cast(updated_at as timestamp)    as updated_at
from ranked
where rn = 1
