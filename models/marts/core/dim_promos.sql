with 
stg_promos as (
    select *
    from {{ref('stg_promos')}}
),

promos as (

    select
        desc_promo,
        id_promo,
        desc_status,
        id_status,
        discount,
        _fivetran_deleted,
        _fivetran_synced
    from stg_promos
)

select * from promos