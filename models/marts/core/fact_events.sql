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
    left join {{ref('dim_event_type')}} T on E.id_event_type = T.id_event_type
    left join {{ref('dim_users')}} U on E.id_user = U.id_user
    left join {{ref('dim_products')}} P on E.id_product = P.id_product
    left join {{ref('dim_tiempo')}} I on id_created_at_utc = I.id_date
)

select *
from events