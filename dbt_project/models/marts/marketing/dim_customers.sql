-- models/marts/marketing/dim_customers.sql
-- Customer 360 dimension — marketing use cases (segmentation, retargeting, CRM)

{{
  config(
    materialized='table',
    cluster_by=["customer_segment", "country_code"],
    labels={"domain": "marketing", "pii": "true"},
    description="Customer master with lifetime metrics and RFM segment — refreshed daily"
  )
}}

with base as (
    select * from {{ ref('int_customer_orders') }}
),

rfm as (
    select
        customer_id,
        -- Recency score 1-5 (5 = most recent)
        ntile(5) over (order by days_since_last_order desc)             as recency_score,
        -- Frequency score 1-5
        ntile(5) over (order by total_orders asc)                       as frequency_score,
        -- Monetary score 1-5
        ntile(5) over (order by total_revenue_eur asc)                  as monetary_score
    from base
    where total_orders > 0
),

final as (
    select
        b.customer_id,
        b.email,
        b.first_name,
        b.last_name,
        b.country_code,
        b.date_of_birth,
        b.is_newsletter_subscribed,
        b.customer_created_at,
        b.customer_age_days,

        -- purchase history
        b.total_orders,
        b.total_revenue_eur,
        b.avg_order_value_eur,
        b.first_order_date,
        b.last_order_date,
        b.customer_lifespan_days,
        b.days_since_last_order,
        b.customer_segment,

        -- RFM
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        coalesce(r.recency_score, 0)
            + coalesce(r.frequency_score, 0)
            + coalesce(r.monetary_score, 0)                             as rfm_total_score,

        -- VIP flag
        case
            when r.monetary_score = 5 and r.frequency_score >= 4 then true
            else false
        end                                                             as is_vip,

        current_timestamp()                                             as updated_at

    from base b
    left join rfm r using (customer_id)
)

select * from final
