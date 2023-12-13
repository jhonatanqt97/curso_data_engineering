with 

soure_users as (
    select* from {{ref('stg_users')}}
),


users as (
    select
        U.id_user,
        U.updated_at_utc,
        U.id_address,
        U.last_name,
        U.created_at_utc,
        U.phone_number,
        U.first_name,
        U.email,
        COUNT(DISTINCT O.id_order) as total_orders,
        U._fivetran_synced
    from soure_users U
     left join {{ref('fact_orders')}} O on O.id_user = U.id_user
    group by U.id_user, U.updated_at_utc, U.id_address, U.last_name, U.created_at_utc, U.phone_number, U.first_name, U.email, U._fivetran_synced
)

select * from users