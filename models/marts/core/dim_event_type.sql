with
stg_event_type as (
    select * 
    from {{ref('stg_event_type')}}
),

event_type as (
    select 
        id_event_type,
        desc_event_type
    from stg_event_type
)

select * from event_type