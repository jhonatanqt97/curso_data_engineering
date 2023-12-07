with 

soure_orders as (
    select* from {{ref('stg_orders')}}
),

soure_users as (
    select* from {{ref('stg_users')}}
),

total_orders as (
    select
        count(distinct(id_order)) As total_orders,
        id_user
    from soure_orders
    group by id_user
),

users as (
    select
        A.id_user,
        updated_at_utc,
        id_address,
        last_name,
        created_at_utc,
        phone_number,
        first_name,
        email,
        B.total_orders,
        _fivetran_synced
    from soure_users as A
    left join total_orders as B on B.id_user =  A.id_user
)

select * from users