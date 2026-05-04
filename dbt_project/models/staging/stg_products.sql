with source as (
    select * from {{ source('raw_ecommerce', 'products') }}
),

renamed as (
    select
        product_id,
        name,
        category,
        sub_category,
        cast(price as numeric)          as price_eur,
        is_active,
        _fivetran_synced                as synced_at
    from source
    where product_id is not null
)

select * from renamed