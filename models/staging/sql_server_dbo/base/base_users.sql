 with 

source as (

    select * from {{ source('sql_server_dbo', 'users') }}

),

renamed as (

    select
        user_id,
        cast(updated_at as timestamp_ltz)as updated_at_utc,
        address_id,
        last_name,
        cast(created_at as timestamp_ltz)as created_at_utc,
        phone_number,
        decode(total_orders,null,'9999',total_orders) AS total_orders,
        first_name,
        email,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
