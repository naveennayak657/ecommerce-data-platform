-- ──────────────────────────────────────────────────
-- Author: Naveen Vishlavath
-- Model: marts/dim_products.sql
--
-- Product dimension table — enriched product catalog
-- with performance metrics derived from events.
--
-- This is a Type 1 SCD (Slowly Changing Dimension)
-- meaning we keep only the latest product state.
--
-- Key metrics:
--   - Total views, orders, revenue per product
--   - Conversion rate (views to orders)
--   - Average order value per product
--   - Product popularity ranking
--   - Cart abandonment rate per product
--
-- Depends on: staging.stg_events
-- ──────────────────────────────────────────────────

with events as (

    select * from {{ ref('stg_events') }}

),

-- get latest product attributes
-- prices change over time — we want the most recent price
product_attributes as (

    select
        product_id,
        product_name,
        category,
        price                                   as current_price,

        -- rank to get latest record per product
        row_number() over (
            partition by product_id
            order by event_timestamp desc
        )                                       as rn

    from events

),

-- aggregate product performance metrics
product_metrics as (

    select
        product_id,

        -- view metrics
        count(case when event_type = 'item_viewed'
                   then event_id end)           as total_views,

        count(distinct case when event_type = 'item_viewed'
                            then user_id end)   as unique_viewers,

        -- cart metrics
        count(case when event_type = 'cart_updated'
                   and cart_action = 'add'
                   then event_id end)           as total_cart_adds,

        count(case when event_type = 'cart_updated'
                   and cart_action = 'remove'
                   then event_id end)           as total_cart_removes,

        -- order metrics
        count(case when event_type = 'order_placed'
                   then event_id end)           as total_orders,

        count(distinct case when event_type = 'order_placed'
                            then user_id end)   as unique_buyers,

        -- revenue metrics
        coalesce(
            sum(case when event_type = 'order_placed'
                     then total_amount end), 0
        )                                       as total_revenue,

        coalesce(
            sum(case when event_type = 'order_placed'
                     then quantity end), 0
        )                                       as total_units_sold,

        -- first and last seen dates
        min(event_timestamp)                    as first_seen_at,
        max(event_timestamp)                    as last_seen_at

    from events
    group by product_id

),

-- join attributes with metrics
final as (

    select
        -- identifiers
        a.product_id,
        a.product_name,
        a.category,
        a.current_price,

        -- view metrics
        m.total_views,
        m.unique_viewers,

        -- cart metrics
        m.total_cart_adds,
        m.total_cart_removes,

        -- order metrics
        m.total_orders,
        m.unique_buyers,
        m.total_units_sold,

        -- revenue
        m.total_revenue,

        -- calculated metrics
        -- average order value for this product
        round(
            case when m.total_orders > 0
                 then m.total_revenue / m.total_orders
                 else 0 end, 2
        )                                       as avg_order_value,

        -- view to order conversion rate
        -- key e-commerce metric — what % of views become orders
        round(
            case when m.total_views > 0
                 then m.total_orders / m.total_views * 100
                 else 0 end, 2
        )                                       as view_to_order