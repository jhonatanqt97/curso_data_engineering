with 
stg_orders as (
    select *
    from {{ref('stg_orders')}}
),

orders as (
    select
        id_order,
        shipping_service,
        shipping_cost_$,
        O.id_address,
        O.created_at_utc,
        O.id_promo,
        estimated_delivery_at_utc,
        order_cost_$,
        O.id_user,
        order_total_$,
        delivered_at,
        id_tracking,
        status,
        LEFT(REPLACE({{ dbt_date.round_timestamp("delivered_at")}}, '-', ''), 8) as id_delivered_at,
        O._fivetran_synced  
    from stg_orders O
    left join {{ref('dim_addresses')}} A on O.id_address = A.id_address
    left join {{ref('dim_promos')}} P on O.id_promo = P.id_promo
    left join {{ref('dim_users')}} U on O.id_user = U.id_user
    left join {{ref('dim_tiempo')}} T on id_delivered_at = T.id_date
    
)

select * from orders
