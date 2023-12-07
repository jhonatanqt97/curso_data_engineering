with 
source_events as (
    select *
    from {{source('sql_server_dbo','events')}}
),

events as (
    select 
         distinct({{ dbt_utils.generate_surrogate_key(['event_type'])}}) as  id_event_type,
         event_type as desc_event_type
    from source_events
)

select*
from events