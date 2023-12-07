with

source as (select * from {{ source('sql_server_dbo', 'promos') }}),



promos as (

    select
        promo_id as desc_promo,
        {{ dbt_utils.generate_surrogate_key(['promo_id'])}} as id_promo,
        status,
        discount,
        _fivetran_deleted,
        _fivetran_synced

    from source

)



select * from promos
