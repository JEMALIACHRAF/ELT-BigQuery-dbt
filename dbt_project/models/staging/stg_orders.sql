-- models/staging/stg_orders.sql
-- Staging layer: clean + rename raw orders
-- Materialized as view for cost efficiency

with source as (
    select * from {{ source('raw_ecommerce', 'orders') }}
),

renamed as (
    select
        -- keys
        order_id,
        customer_id,

        -- dimensions
        status                                                          as order_status,
        payment_method,
        shipping_country                                                as country_code,

        -- amounts
        cast(total_amount as numeric)                                   as total_amount_eur,
        cast(shipping_cost as numeric)                                  as shipping_cost_eur,
        cast(discount_amount as numeric)                                as discount_amount_eur,
        total_amount - shipping_cost - discount_amount                  as net_revenue_eur,

        -- dates
        cast(created_at as timestamp)                                   as order_created_at,
        cast(updated_at as timestamp)                                   as order_updated_at,
        date(created_at)                                                as order_date,

        -- metadata
        _fivetran_synced                                                as synced_at

    from source
    where order_id is not null
      and created_at is not null
)

select * from renamed
