select
    merchant_id,
    name                             as merchant_name,
    category,
    cast(zone_id as integer)         as zone_id,
    cast(onboarded_date as date)     as onboarded_date,
    cast(commission_rate as {{ dbt.type_numeric() }})  as commission_rate
from {{ source('raw', 'merchants') }}
