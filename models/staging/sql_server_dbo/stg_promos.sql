with

source as (select * from {{ source('sql_server_dbo', 'promos') }}),



renamed as (

    select
        promo_id as des_promo,
        {{ dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_id_key,
        {{ dbt_utils.generate_surrogate_key(['status'])}} as status_id,
        discount,
        status as desc_status,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
