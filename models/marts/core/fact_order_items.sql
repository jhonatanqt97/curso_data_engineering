with 
source as (
    select * 
    from {{ref('stg_order_items')}}
),

order_items as (
    select
        id_order,
        id_product,
        quantity,
        _fivetran_synced
    from source
)


select * from order_items
