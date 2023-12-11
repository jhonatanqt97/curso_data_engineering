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
       distinct(status) as desc_status

    from promos

    union all

    select 
       distinct(status) as desc_status
    from orders

),

status as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_status'])}} as id_status,
        desc_status
    from combinacion
)

select * from status