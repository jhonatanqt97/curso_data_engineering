with 

orders as (
    select  *
    from {{ref('fact_orders')}}
),

operacion as (
    SELECT 
        id_created_at,
        sum(shipping_cost_$),
       sum( order_total_$),
       sum(order_cost_$)
    from orders
    group by id_created_at
)

select * from operacion