-- models/intermediate/int_orders_enriched.sql
-- Join orders + items + products to compute order-level metrics
-- Ephemeral: no table created, inlined into downstream models

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

items_with_products as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        p.category                                                      as product_category,
        p.name                                                          as product_name,
        oi.quantity,
        oi.unit_price_eur,
        oi.line_total_eur,
        oi.net_line_total_eur
    from order_items oi
    left join products p using (product_id)
),

order_aggregates as (
    select
        order_id,
        count(distinct order_item_id)                                   as item_count,
        sum(quantity)                                                   as total_units,
        sum(net_line_total_eur)                                         as items_net_total_eur,
        array_agg(distinct product_category ignore nulls)               as product_categories,
        count(distinct product_category)                                as nb_categories
    from items_with_products
    group by 1
),

final as (
    select
        o.*,
        agg.item_count,
        agg.total_units,
        agg.items_net_total_eur,
        agg.product_categories,
        agg.nb_categories,

        -- derived flags
        case
            when o.order_status = 'delivered' then true
            else false
        end                                                             as is_completed,

        case
            when o.order_status in ('cancelled', 'refunded') then true
            else false
        end                                                             as is_lost

    from orders o
    left join order_aggregates agg using (order_id)
)

select * from final
