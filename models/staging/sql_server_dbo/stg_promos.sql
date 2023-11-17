with 

source as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

renamed as (

    select
        md5(promo_id) as promo_id,
        discount,
        status,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
