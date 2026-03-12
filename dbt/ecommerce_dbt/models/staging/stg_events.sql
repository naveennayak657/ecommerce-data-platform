-- ──────────────────────────────────────────────────
-- Author: Naveen Vishlavath
-- Model: staging/stg_events.sql
--
-- Staging model for raw e-commerce events.
-- This is the first transformation layer after raw data
-- lands in Snowflake from our Delta Lake Gold layer.
--
-- What this model does:
--   - Renames columns to consistent naming convention
--   - Casts data types explicitly
--   - Filters out invalid records
--   - Deduplicates on event_id (keeps latest)
--   - Adds derived columns for easier downstream use
--
-- Depends on: source('raw', 'events')
-- ──────────────────────────────────────────────────

with raw_events as (

    select * from {{ source('raw', 'events') }}

),

cleaned as (

    select
        -- identifiers
        event_id                                    as event_id,
        user_id                                     as user_id,
        session_id                                  as session_id,

        -- event details
        lower(trim(event_type))                     as event_type,
        product_id                                  as product_id,
        lower(trim(product_name))                   as product_name,
        lower(trim(category))                       as category,

        -- financials
        cast(price as numeric(10, 2))               as price,
        coalesce(cast(quantity as int), 0)          as quantity,
        coalesce(cast(total_amount as numeric(10,2)),
                 cast(price as numeric(10,2)))      as total_amount,

        -- payment details
        lower(trim(payment_method))                 as payment_method,
        lower(trim(status))                         as payment_status,

        -- cart details
        lower(trim(action))                         as cart_action,

        -- timestamps
        cast(event_timestamp as timestamp_ntz)      as event_timestamp,
        cast(ingested_at as timestamp_ntz)          as ingested_at,
        cast(ingestion_date as date)                as ingestion_date,

        -- derived columns — useful for downstream models
        date_trunc('hour', event_timestamp)         as event_hour,
        date_trunc('day', event_timestamp)          as event_date,
        dayofweek(event_timestamp)                  as day_of_week,
        hour(event_timestamp)                       as hour_of_day

    from raw_events

    where
        -- filter out records with missing critical fields
        event_id        is not null
        and event_type  is not null
        and user_id     is not null
        and price       > 0
        and event_type  in (
            'order_placed',
            'item_viewed',
            'cart_updated',
            'payment_processed'
        )

),

deduplicated as (

    select *
    from cleaned

    -- remove duplicate events keeping the latest record
    -- duplicates happen when kafka delivers a message more than once
    -- this is called "at least once delivery" — common in streaming
    qualify row_number() over (
        partition by event_id
        order by ingested_at desc
    ) = 1

)

select * from deduplicated