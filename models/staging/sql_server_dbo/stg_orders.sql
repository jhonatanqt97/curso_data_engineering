

WITH source AS (
    SELECT * FROM {{ source('sql_server_dbo','orders') }}
    

    ),

orders as (

    select
        order_id,
        decode(shipping_service,'','vacio',shipping_service) AS shipping_service,
        shipping_cost,
        address_id,
        created_at,
        decode(promo_id,'','9999', promo_id) AS promo_id,
        decode(estimated_delivery_at,null,'9999',estimated_delivery_at) AS estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        decode(delivered_at,null,'9999',delivered_at) AS delivered_at,
        decode(tracking_id,'','vacio',tracking_id) AS tracking_id,
        status as des_status,
        {{ dbt_utils.generate_surrogate_key(['status'])}} as status,
        _fivetran_deleted,
        _fivetran_synced

    from source

 
),

orders_casted as (

    select
        order_id as id_order,
        shipping_service,
        shipping_cost as shipping_cost_$,
        address_id as id_address,
        cast(created_at as timestamp_ltz) as created_at_utc,
        cast(
            case
             when promo_id= '9999' then '9999'
             else {{ dbt_utils.generate_surrogate_key(['promo_id'])}}
             end as varchar(50) 
        ) as id_promo,
        cast(estimated_delivery_at as timestamp_ltz) as estimated_delivery_at_utc,
        order_cost as order_cost_$,
        user_id as id_user,
        order_total as order_total_$,
        cast(delivered_at as timestamp_ltz) as delivered_at,
        tracking_id as id_tracking,
        des_status,
        status,
        _fivetran_deleted,
        _fivetran_synced 

    from orders

)
   

select * from orders_casted
