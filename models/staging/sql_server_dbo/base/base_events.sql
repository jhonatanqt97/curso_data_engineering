with 

source as (

    select * from {{ source('sql_server_dbo', 'events') }}

),

renamed as (

    select
        event_id,
        page_url,
        event_type,
        user_id,
        decode(product_id,'','9999', product_id) AS product_id,
        session_id,
         cast(created_at as timestamp_ltz)as created_at_utc,
        decode(order_id,'','9999', order_id) AS order_id,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
