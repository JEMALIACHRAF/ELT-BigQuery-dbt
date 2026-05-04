-- models/marts/product/fct_product_performance.sql
-- Product performance metrics — refreshed daily

{{
  config(
    materialized='table',
    cluster_by=["category", "order_date"],
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "month"
    },
    labels={"domain": "product"},
    description="Monthly product performance: revenue, units, return rate by category"
  )
}}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_date, order_status, country_code
    from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

joined as (
    select
        date_trunc(o.order_date, month)                                 as order_date,
        p.product_id,
        p.name                                                          as product_name,
        p.category,
        p.sub_category,
        o.country_code,
        oi.quantity,
        oi.unit_price_eur,
        oi.net_line_total_eur,
        o.order_status
    from order_items oi
    join orders o using (order_id)
    join products p using (product_id)
),

aggregated as (
    select
        order_date,
        product_id,
        product_name,
        category,
        sub_category,
        country_code,

        count(*)                                                        as nb_order_lines,
        sum(quantity)                                                   as units_sold,
        sum(net_line_total_eur)                                         as net_revenue_eur,
        avg(unit_price_eur)                                             as avg_selling_price_eur,

        countif(order_status in ('cancelled', 'refunded'))              as nb_returns,
        safe_divide(
            countif(order_status in ('cancelled', 'refunded')),
            count(*)
        )                                                               as return_rate

    from joined
    group by 1, 2, 3, 4, 5, 6
)

select * from aggregated
