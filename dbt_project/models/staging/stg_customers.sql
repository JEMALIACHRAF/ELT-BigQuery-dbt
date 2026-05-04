-- models/staging/stg_customers.sql

with source as (
    select * from {{ source('raw_ecommerce', 'customers') }}
),

renamed as (
    select
        customer_id,
        lower(trim(email))                                              as email,
        initcap(first_name)                                             as first_name,
        initcap(last_name)                                              as last_name,
        country_code,
        cast(date_of_birth as date)                                     as date_of_birth,

        -- computed
        date_diff(current_date(), cast(created_at as date), day)        as customer_age_days,
        cast(created_at as timestamp)                                   as customer_created_at,

        -- flags
        is_email_verified,
        is_newsletter_subscribed,

        _fivetran_synced                                                as synced_at

    from source
    where customer_id is not null
      and email is not null
)

select * from renamed
