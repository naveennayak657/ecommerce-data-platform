-- ──────────────────────────────────────────────────
-- Author: Naveen Vishlavath
-- Model: marts/fct_user_behavior.sql
--
-- User behavior fact table — tracks user journey
-- across the entire e-commerce funnel.
--
-- Key metrics:
--   - Session level engagement
--   - Funnel conversion (view → cart → order)
--   - Cart abandonment detection
--   - User segmentation (new vs returning)
--   - Session duration and activity
--
-- Depends on: staging.stg_events
-- ──────────────────────────────────────────────────

with events as (

    select * from {{ ref('stg_events') }}

),

-- aggregate behavior at session level
-- one row per user per session
session_stats as (

    select
        user_id,
        session_id,

        -- session timestamps
        min(event_timestamp)                    as session_start,
        max(event_timestamp)                    as session_end,
        min(event_date)                         as session_date,
        min(day_of_week)                        as session_day_of_week,
        min(hour_of_day)                        as session_start_hour,

        -- session duration in minutes
        datediff(
            'minute',
            min(event_timestamp),
            max(event_timestamp)
        )                                       as session_duration_minutes,

        -- event counts per session
        count(event_id)                         as total_events,

        count(case when event_type = 'item_viewed'
                   then event_id end)           as total_views,

        count(case when event_type = 'cart_updated'
                   and cart_action = 'add'
                   then event_id end)           as total_cart_adds,

        count(case when event_type = 'cart_updated'
                   and cart_action = 'remove'
                   then event_id end)           as total_cart_removes,

        count(case when event_type = 'order_placed'
                   then event_id end)           as total_orders,

        count(case when event_type = 'payment_processed'
                   then event_id end)           as total_payments,

        count(case when event_type = 'payment_processed'
                   and payment_status = 'success'
                   then event_id end)           as successful_payments,

        -- revenue metrics per session
        coalesce(sum(case when event_type = 'order_placed'
                          then total_amount end), 0)
                                                as session_revenue,

        coalesce(sum(case when event_type = 'order_placed'
                          then quantity end), 0)
                                                as total_items_ordered,

        -- unique products interacted with
        count(distinct product_id)              as unique_products_viewed,

        -- unique categories browsed
        count(distinct category)                as unique_categories_browsed

    from events
    group by user_id, session_id

),

-- add funnel conversion flags and user level metrics
final as (

    select
        -- identifiers
        user_id,
        session_id,

        -- session timing
        session_start,
        session_end,
        session_date,
        session_day_of_week,
        session_start_hour,
        session_duration_minutes,

        -- engagement metrics
        total_events,
        total_views,
        total_cart_adds,
        total_cart_removes,
        total_orders,
        total_payments,
        successful_payments,
        unique_products_viewed,
        unique_categories_browsed,

        -- revenue
        session_revenue,
        total_items_ordered,

        -- funnel conversion flags
        -- did user view something?
        case when total_views > 0
             then true else false
        end                                     as did_view,

        -- did user add to cart?
        case when total_cart_adds > 0
             then true else false
        end                                     as did_add_to_cart,

        -- did user place an order?
        case when total_orders > 0
             then true else false
        end                                     as did_order,

        -- did user complete payment?
        case when successful_payments > 0
             then true else false
        end                                     as did_complete_payment,

        -- cart abandonment — added to cart but never ordered
        -- one of the most important e-commerce metrics
        case when total_cart_adds > 0
              and total_orders = 0
             then true else false
        end                                     as is_cart_abandoned,

        -- session value tier
        case
            when session_revenue >= 100 then 'high_value'
            when session_revenue >= 50  then 'medium_value'
            when session_revenue > 0    then 'low_value'
            else 'no_purchase'
        end                                     as session_value_tier,

        -- user session rank — 1 = first session ever
        row_number() over (
            partition by user_id
            order by session_start
        )                                       as user_session_rank,

        -- flag new vs returning users
        case
            when row_number() over (
                partition by user_id
                order by session_start
            ) = 1 then 'new'
            else 'returning'
        end                                     as user_type,

        -- cumulative revenue per user across all sessions
        sum(session_revenue) over (
            partition by user_id
            order by session_start
            rows between unbounded preceding and current row
        )                                       as user_cumulative_revenue,

        -- cumulative sessions per user
        count(session_id) over (
            partition by user_id
            order by session_start
            rows between unbounded preceding and current row
        )                                       as user_total_sessions,

        -- metadata
        current_timestamp()                     as ingested_at

    from session_stats

)

select * from final