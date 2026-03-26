with source as (
    select *
    from {{ ref('stg_ga4__events') }}
),

unnested as (
    select
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        param.key as param_key,
        param.value.string_value as param_string_value,
        param.value.int_value as param_int_value,
        param.value.float_value as param_float_value,
        param.value.double_value as param_double_value
    from source,
    unnest(event_params) as param
)

select * from unnested