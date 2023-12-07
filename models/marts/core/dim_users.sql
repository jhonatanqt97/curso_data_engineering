with 

soure_users as (
    select* from {{ref('stg_users')}}
),


users as (
    select
        id_user,
        updated_at_utc,
        id_address,
        last_name,
        created_at_utc,
        phone_number,
        first_name,
        email,
        _fivetran_synced
    from soure_users 
 
)

select * from users