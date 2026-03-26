with events_with_params as (
    select *
    from {{ ref('int_ga4__events_with_params') }}
),

-- Use native session_id if present, else generate a surrogate session key
sessions_prep as (
    select
        *,
        coalesce(
            session_id,
            ga_session_id,
            -- fallback: create a dummy id based on user and timestamp boundaries
            null
        ) as session_key,
        -- determine if this event starts a new session
        case
            when session_id is not null or ga_session_id is not null
                then 0   -- we rely on the native id, will handle later
            else
                -- time-based: gap > 30 minutes (1800 seconds) marks a new session
                lag(event_timestamp) over (
                    partition by user_pseudo_id
                    order by event_timestamp
                ) is null
                or (event_timestamp - lag(event_timestamp) over (
                    partition by user_pseudo_id
                    order by event_timestamp
                )) > 1800000000   -- microseconds: 30*60*1e6
        end as is_new_session
    from events_with_params
),

-- Assign a session ID for time-based sessions
sessions_assigned as (
    select
        *,
        case
            when session_key is not null then session_key
            else
                sum(case when is_new_session then 1 else 0 end) over (
                    partition by user_pseudo_id
                    order by event_timestamp
                    rows unbounded preceding
                )
        end as session_id_final
    from sessions_prep
),

-- Now aggregate at session level
sessions as (
    select
        session_id_final as session_id,
        user_pseudo_id,
        min(event_timestamp) as session_start_timestamp,
        max(event_timestamp) as session_end_timestamp,
        timestamp_diff(
            timestamp_micros(max(event_timestamp)),
            timestamp_micros(min(event_timestamp)),
            second
        ) as session_duration_seconds,
        count(*) as event_count,
        -- first and last event names
        array_agg(event_name order by event_timestamp limit 1)[offset(0)] as first_event_name,
        array_agg(event_name order by event_timestamp desc limit 1)[offset(0)] as last_event_name,
        -- landing page (first page_location)
        array_agg(page_location order by event_timestamp limit 1)[offset(0)] as landing_page,
        -- ecommerce aggregates (from items / ecommerce)
        sum(if(event_name = 'purchase', ecommerce.purchase_revenue, 0)) as purchase_revenue,
        sum(if(event_name = 'purchase', ecommerce.total_item_quantity, 0)) as total_items_purchased,
        -- traffic source (first event's campaign info)
        array_agg(traffic_source order by event_timestamp limit 1)[offset(0)] as first_traffic_source,
        array_agg(session_traffic_source_last_click order by event_timestamp limit 1)[offset(0)] as first_session_traffic_source
    from events_with_params
    group by session_id_final, user_pseudo_id
)

select * from sessions