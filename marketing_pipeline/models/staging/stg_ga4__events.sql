with source as (
    select *
    from {{ source('ga4', 'raw_events') }}
)

select
    event_date,
    event_timestamp,
    event_name,
    event_previous_timestamp,
    event_value_in_usd,
    event_bundle_sequence_id,
    event_server_timestamp_offset,
    user_id,
    user_pseudo_id,
    user_first_touch_timestamp,
    device,
    geo,
    app_info,
    traffic_source,
    collected_traffic_source,
    session_traffic_source_last_click,
    ecommerce,
    items,
    stream_id,
    platform,
    event_dimensions,
    privacy_info,
    user_properties,
    user_ltv,
    is_active_user,
    batch_event_index,
    batch_page_id,
    batch_ordering_id,
    publisher

from source