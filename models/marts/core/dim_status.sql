with


promos as (
    select*
    from {{ref('stg_promos')}}
),

orders as (
    select*
    from {{ref('stg_orders')}}
),

combinacion as (
    select 
        distinct(id_status) as id_status,
        desc_status

    from promos

    union all

    select 
        distinct({{ dbt_utils.generate_surrogate_key(['status'])}}) as  id_status,
        des_status

    from orders

)

select * from combinacion
