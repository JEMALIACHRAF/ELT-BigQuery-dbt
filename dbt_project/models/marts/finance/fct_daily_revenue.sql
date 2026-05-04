-- models/marts/finance/fct_daily_revenue.sql
-- Fact table: daily revenue KPIs for finance dashboards
-- Materialized as partitioned + clustered BigQuery table

{{
  config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["country_code", "order_status"],
    labels={"domain": "finance", "freshness": "daily"},
    description="Daily revenue aggregated by country and status — source of truth for finance reporting"
  )
}}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

daily as (
    select
        order_date,
        country_code,
        order_status,

        -- volume
        count(distinct order_id)                                        as nb_orders,
        count(distinct customer_id)                                     as nb_customers,
        sum(total_units)                                                as nb_units_sold,

        -- revenue
        sum(net_revenue_eur)                                            as gross_revenue_eur,
        sum(case when is_completed then net_revenue_eur else 0 end)     as confirmed_revenue_eur,
        sum(case when is_lost      then net_revenue_eur else 0 end)     as lost_revenue_eur,
        sum(discount_amount_eur)                                        as total_discounts_eur,
        sum(shipping_cost_eur)                                          as total_shipping_eur,
        avg(net_revenue_eur)                                            as avg_order_value_eur,

        -- rates
        safe_divide(
            countif(is_lost),
            count(distinct order_id)
        )                                                               as cancellation_rate,

        safe_divide(
            countif(is_completed),
            count(distinct order_id)
        )                                                               as completion_rate

    from orders
    group by 1, 2, 3
)

select * from daily
