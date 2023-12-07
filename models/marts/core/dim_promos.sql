with 


promos as (

    select
        desc_promo,
        id_promo,
        desc_status,
        id_status,
        discount,
        _fivetran_synced
    from {{ref('stg_promos')}} 
   
)

select * from promos