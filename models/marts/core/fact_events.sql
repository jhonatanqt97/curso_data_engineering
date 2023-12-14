with 

stg_events as (
    select * 
    from {{ref('stg_sql_server_dbo_events')}}
),

events as (
    select
        E.id_event,
        page_url,
        E.id_event_type,
        E.id_user,
        E.id_product,
        id_session,
        LEFT(REPLACE({{ dbt_date.round_timestamp("E.created_at_utc")}}, '-', ''), 8) as id_created_at_utc,
        E._fivetran_synced
    from stg_events E
    
)

select *
from events