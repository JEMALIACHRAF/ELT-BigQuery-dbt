-- models/intermediate/int_customer_orders.sql
-- Aggregate order history per customer for CLV computation

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
    where is_completed = true
),

customer_orders as (
    select
        customer_id,
        count(distinct order_id)                                        as total_orders,
        sum(net_revenue_eur)                                            as total_revenue_eur,
        avg(net_revenue_eur)                                            as avg_order_value_eur,
        min(order_date)                                                 as first_order_date,
        max(order_date)                                                 as last_order_date,
        date_diff(max(order_date), min(order_date), day)                as customer_lifespan_days,

        -- recency / frequency for RFM
        date_diff(current_date(), max(order_date), day)                 as days_since_last_order,
        safe_divide(
            count(distinct order_id),
            nullif(date_diff(max(order_date), min(order_date), day), 0)
        ) * 30                                                          as orders_per_month

    from orders
    group by 1
),

final as (
    select
        c.*,
        coalesce(co.total_orders, 0)                                    as total_orders,
        coalesce(co.total_revenue_eur, 0)                               as total_revenue_eur,
        co.avg_order_value_eur,
        co.first_order_date,
        co.last_order_date,
        co.customer_lifespan_days,
        co.days_since_last_order,
        co.orders_per_month,

        -- segment
        case
            when co.total_orders is null              then 'no_purchase'
            when co.days_since_last_order <= 30       then 'active'
            when co.days_since_last_order <= 90       then 'at_risk'
            else                                           'churned'
        end                                                             as customer_segment

    from customers c
    left join customer_orders co using (customer_id)
)

select * from final
