select
    courier_id,
    cast(hire_date as date)          as hire_date,
    vehicle_type,
    cast(zone_id as integer)         as zone_id,
    status                           as courier_status
from {{ source('raw', 'couriers') }}
