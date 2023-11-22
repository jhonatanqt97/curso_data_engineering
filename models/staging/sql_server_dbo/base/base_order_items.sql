with 

source as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

renamed as (

    select
        concat(order_id,product_id) as order_product_id,
        quantity,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
