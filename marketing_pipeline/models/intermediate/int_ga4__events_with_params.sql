with events as (
    select *
    from {{ ref('stg_ga4__events') }}
),

params as (
    select *
    from {{ ref('stg_ga4__event_params') }}
),

joined as (
    select
        e.* except(event_params),
        {{ extract_param('page_location', 'string') }},
        {{ extract_param('page_referrer', 'string') }},
        {{ extract_param('session_id', 'string') }},
        {{ extract_param('ga_session_id', 'string') }},
        {{ extract_param('ga_session_number', 'int') }},
        {{ extract_param('page_title', 'string') }},
        {{ extract_param('engagement_time_msec', 'int') }},
        {{ extract_param('entrances', 'int') }}
    from events e
    left join params p
        on e.event_date = p.event_date
        and e.event_timestamp = p.event_timestamp
        and e.user_pseudo_id = p.user_pseudo_id
        and e.event_name = p.event_name
    group by all
)

select * from joined