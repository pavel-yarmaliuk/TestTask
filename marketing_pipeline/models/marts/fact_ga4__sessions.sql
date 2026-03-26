with sessions as (
    select *
    from {{ ref('int_ga4__sessions') }}
),

enriched as (
    select
        *,
        first_traffic_source.source as traffic_source_source,
        first_traffic_source.medium as traffic_source_medium,
        first_traffic_source.name as traffic_source_campaign,
        first_session_traffic_source.google_ads_campaign.campaign_name as google_ads_campaign,
        first_session_traffic_source.cross_channel_campaign.default_channel_group as channel_group
    from sessions
)

select
    session_id,
    user_pseudo_id,
    session_start_timestamp,
    session_end_timestamp,
    session_duration_seconds,
    event_count,
    first_event_name,
    last_event_name,
    landing_page,
    purchase_revenue,
    total_items_purchased,
    traffic_source_source,
    traffic_source_medium,
    traffic_source_campaign,
    google_ads_campaign,
    channel_group,
    case when event_count = 1 then 1 else 0 end as bounce_session,
    case when purchase_revenue > 0 then 1 else 0 end as conversion_session

from enriched