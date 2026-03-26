with events as (
    select *
    from {{ ref('int_ga4__events_with_params') }}
)

select
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    user_id,
    session_id,
    ga_session_id,
    page_location,
    page_referrer,
    page_title,
    engagement_time_msec,
    entrances,
    device.category as device_category,
    device.mobile_brand_name,
    device.mobile_model_name,
    device.operating_system,
    device.operating_system_version,
    device.language,
    device.browser,
    device.browser_version,
    geo.country,
    geo.region,
    geo.city,
    app_info.id as app_id,
    app_info.version as app_version,
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name as campaign,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
    collected_traffic_source.manual_campaign_name,
    ecommerce.purchase_revenue,
    ecommerce.total_item_quantity,
    ecommerce.transaction_id,
    to_json_string(items) as items_json,
    user_properties,
    stream_id,
    platform

from events