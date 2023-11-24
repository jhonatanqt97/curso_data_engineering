with 
products as (
    select *
    from {{ref('stg_products')}}
),

productos as (

    select
       id_product,
       price_$,
       name,
       inventory,
       _fivetran_deleted,
       _fivetran_synced
    from products
)

select * from productos
