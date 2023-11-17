with 

source as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

renamed as (

    select
        order_id,
        decode(shipping_service,'','vacio',shipping_service) AS shipping_service,
        shipping_cost,
        address_id,
        created_at,
        decode(promo_id,'','vacio',promo_id) AS promo_id,
        decode(estimated_delivery_at,null,'9999',estimated_delivery_at) AS estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        decode(delivered_at,null,'9999',delivered_at) AS delivered_at,
        decode(tracking_id,'','vacio',tracking_id) AS tracking_id,
        status,
        _fivetran_deleted,
        _fivetran_synced

    from source
 
)

select * from renamed
