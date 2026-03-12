-- ──────────────────────────────────────────────────
-- Author: Naveen Vishlavath
-- Model: marts/fct_payments.sql
--
-- Payments fact table — tracks all payment transactions.
-- Contains one row per payment event with success/failure
-- status and payment method breakdown.
--
-- Fixes from v1:
--   - Fixed column names to match stg_events
--   - Added payment retry analysis
--   - Added avg payment value by method
--
-- Key metrics:
--   - Payment success rate by method
--   - Failed payment tracking
--   - Revenue by payment method
--   - Payment retry detection
--
-- Depends on: staging.stg_events
-- ──────────────────────────────────────────────────

with payments as (

    select * from {{ ref('stg_events') }}
    where event_type = 'payment_processed'

),

final as (

    select
        -- identifiers
        event_id                                as payment_id,
        user_id                                 as user_id,
        session_id                              as session_id,
        product_id                              as product_id,
        product_name                            as product_name,
        category                                as category,

        -- payment details
        payment_method                          as payment_method,
        payment_status                          as payment_status,

        -- financials — using correct stg_events column names
        price                                   as unit_price,
        quantity                                as quantity,
        total_amount                            as payment_amount,

        -- boolean flags — easier for analysts to filter
        case
            when payment_status = 'success' then true
            else false
        end                                     as is_successful,

        case
            when payment_status = 'failed' then true
            else false
        end                                     as is_failed,

        -- timestamps — using correct stg_events column names
        event_timestamp                         as paid_at,
        event_date                              as payment_date,
        event_hour                              as payment_hour,
        day_of_week                             as payment_day_of_week,

        -- running success rate per payment method
        -- shows which payment methods are most reliable over time
        round(
            avg(case when payment_status = 'success' then 1.0
                     else 0.0 end
            ) over (
                partition by payment_method
                order by event_timestamp
                rows between unbounded preceding and current row
            ) * 100, 2
        )                                       as running_success_rate_pct,

        -- total revenue per payment method
        sum(case when payment_status = 'success'
                 then total_amount else 0 end
        ) over (
            partition by payment_method
            order by event_timestamp
            rows between unbounded preceding and current row
        )                                       as running_revenue_by_method,

        -- average payment value by method
        -- shows which payment methods are used for bigger purchases
        round(
            avg(total_amount) over (
                partition by payment_method
            ), 2
        )                                       as avg_payment_value_by_method,

        -- payment attempts per session
        -- detects users retrying after a failed payment
        count(*) over (
            partition by user_id, session_id
            order by event_timestamp
            rows between unbounded preceding and current row
        )                                       as payment_attempts_in_session,

        -- flag retry attempts — attempt > 1 means user retried
        case
            when count(*) over (
                partition by user_id, session_id
                order by event_timestamp
                rows between unbounded preceding and current row
            ) > 1 then true
            else false
        end                                     as is_retry_attempt,

        -- metadata
        ingested_at                             as ingested_at

    from payments

)

select * from final