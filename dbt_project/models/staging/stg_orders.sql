-- One row per order. Duplicates removed, text cleaned and typed, and every known problem is FLAGGED (not deleted)
-- so that marts can decide what to count and the data-quality report can count what was found.
with ranked as (
    select *,
           row_number() over (partition by order_id order by _loaded_at desc) as rn
    from {{ source('raw', 'orders') }}
),
typed as (
    select
        order_id,
        nullif(customer_id, '')                      as customer_id,
        merchant_id,
        nullif(courier_id, '')                       as courier_id,
        cast(zone_id as integer)                     as zone_id,
        {{ try_cast_timestamp('placed_ts') }}             as placed_ts,
        {{ try_cast_timestamp('completed_ts') }}          as completed_ts,
        lower(trim(status))                          as status,
        cast(subtotal as {{ dbt.type_numeric() }})                    as subtotal,        -- money is an exact decimal, never a float
        cast(delivery_fee as {{ dbt.type_numeric() }})                as delivery_fee,
        cast(discount as {{ dbt.type_numeric() }})                    as discount,
        payment_method,
        cast(promised_minutes as integer)            as promised_minutes
    from ranked
    where rn = 1
),
flagged as (
    select t.*,
        (t.placed_ts is null)                                                   as flag_bad_placed_ts,
        (t.subtotal <= 0)                                                       as flag_nonpositive_subtotal,
        (t.customer_id is null)                                                 as flag_null_customer,
        (t.courier_id is not null and c.courier_id is null)                     as flag_orphan_courier,
        (t.status = 'delivered' and t.completed_ts < t.placed_ts)               as flag_completed_before_placed
    from typed t
    left join {{ ref('stg_couriers') }} c on c.courier_id = t.courier_id
)
select *,
       not (flag_bad_placed_ts or flag_nonpositive_subtotal or flag_null_customer
            or flag_orphan_courier or flag_completed_before_placed) as is_valid
from flagged
