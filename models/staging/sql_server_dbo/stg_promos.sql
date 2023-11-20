with 

source as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

renamed as (

    select
       {{ dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_id_key,
        promo_id as desc_promo_id,
        discount,
        status,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed

union all 
(
    select 
    '9999'as promo_id, 'no promo' as desc_promo_id, '0' as discount, 'active' as status, '0' as _fivetran_deleted, '0'as _fivetran_synced
)
