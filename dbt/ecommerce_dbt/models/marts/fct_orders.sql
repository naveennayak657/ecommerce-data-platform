-- ──────────────────────────────────────────────────
-- Author: Naveen Vishlavath
-- Model: marts/fct_orders.sql
--
-- Orders fact table — core business metric table.
-- Contains one row per order event with all relevant
-- dimensions and metrics for analysis.
--
-- Improvements:
--   - Added user lifetime value calculation
--   - Added order rank per user
--   - Added revenue metrics
--
-- Depends on: staging.stg_events
-- ──────────────────────────────────────────────────

with orders as (

    select * from {{ ref('stg_events') }}
    where event_type = 'order_placed'

),

final as (

    select
        -- identifiers
        event_id                                as order_id,
        user_id                                 as user_id,
        session_id                              as session_id,
        product_id                              as product_id,
        product_name                            as product_name,
        category                                as category,

        -- financials
        price                                   as unit_price,
        quantity                                as quantity,
        total_amount                            as order_value,

        -- timestamps
        event_timestamp                         as ordered_at,
        event_date                              as order_date,
        event_hour                              as order_hour,
        day_of_week                             as order_day_of_week,
        hour_of_day                             as order_hour_of_day,

        -- order value tier — useful for marketing segmentation
        case
            when total_amount >= 100 then 'high'
            when total_amount >= 50  then 'medium'
            else 'low'
        end                                     as order_value_tier,

        -- flag weekend orders — useful for marketing analysis
        case
            when dayofweek(event_timestamp) in (1, 7) then true
            else false
        end                                     as is_weekend_order,

        -- user lifetime value up to this order
        -- shows how much each user has spent in total
        sum(total_amount) over (
            partition by user_id
            order by event_timestamp
            rows between unbounded preceding and current row
        )                                       as user_lifetime_value,

        -- order rank per user — 1 = first order ever
        -- useful for identifying new vs returning customers
        row_number() over (
            partition by user_id
            order by event_timestamp
        )                                       as user_order_rank,

        -- flag if this is the user's first order
        case
            when row_number() over (
                partition by user_id
                order by event_timestamp
            ) = 1 then true
            else false
        end                                     as is_first_order,

        -- metadata
        ingested_at                             as ingested_at

    from orders

)

select * from final