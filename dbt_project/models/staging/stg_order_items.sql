-- models/staging/stg_order_items.sql

with source as (
    select * from {{ source('raw_ecommerce', 'order_items') }}
),

renamed as (
    select
        order_item_id,
        order_id,
        product_id,
        cast(quantity as int64)                                         as quantity,
        cast(unit_price as numeric)                                     as unit_price_eur,
        cast(quantity as numeric) * cast(unit_price as numeric)        as line_total_eur,
        cast(discount_pct as numeric)                                   as discount_pct,
        cast(quantity as numeric) * cast(unit_price as numeric)
            * (1 - coalesce(cast(discount_pct as numeric), 0) / 100)   as net_line_total_eur,

        _fivetran_synced                                                as synced_at
    from source
    where order_item_id is not null
)

select * from renamed
