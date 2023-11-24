with 

source as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

order_items as (

    select
        concat(order_id,product_id) as id_order_product,
        quantity,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from order_items
