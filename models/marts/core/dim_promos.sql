with 


promos as (

    select
        desc_promo,
        id_promo,
        status,
        discount,
        _fivetran_synced
    from {{ref('stg_promos')}} 
   
)

select * from promos