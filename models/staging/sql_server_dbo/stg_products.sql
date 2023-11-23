with
source as (
select *  from {{ source('sql_server_dbo', 'products') }}
),



products as (

    select
        product_id as id_product,
        price as price_$,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from products
