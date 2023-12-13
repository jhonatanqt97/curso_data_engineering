with 

events as (
    select  *
    from {{ref('fact_events')}}
),

agregada as (
    SELECT
        E.id_event, 
        E.id_user,
        U.first_name,
        U.last_name,
        E.page_url,
        V.desc_event_type,
        MAX(T.fecha_forecast) AS ultima_actividad,
        P.id_product,
        P.name AS product_name,
        T.fecha_forecast as created_at
    from events E
     left join {{ref('dim_users')}} U on U.id_user = E.id_user
     left join {{ref('dim_products')}} P on P.id_product = E.id_product
     left join {{ref('dim_event_type')}} V on V.id_event_type = E.id_event_type
     left join {{ref('dim_tiempo')}} T on T.id_date = E.id_created_at_utc
    GROUP BY E.id_session, E.id_user, U.first_name, U.last_name,E.id_event,E.page_url,V.desc_event_type,P.id_product,product_name,created_at 
)

select * from agregada