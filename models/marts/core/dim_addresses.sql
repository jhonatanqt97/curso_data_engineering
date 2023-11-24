with 
stg_addresses as (
    select *
    from {{ref('stg_addresses')}}
),

address as (

    select
        id_address,
        zipcode,
        country,
        address,
        state,
        _fivetran_deleted,
        _fivetran_synced
    from stg_addresses
)

select * from address