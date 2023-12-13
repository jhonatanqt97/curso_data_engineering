with
fct_order_items as (
    select *
    from {{(ref('fact_order_items'))}}
),

agregada as (
    select 
        A.state,
        P.id_product,
        P.name as product_name,
        sum(order_total_$)
    from fct_order_items I
    left join {{ref('dim_products')}} P on I.id_product = P.id_product
    left join {{ref('fact_orders')}} O on I.id_order = O.id_order 
    left join {{ref('dim_addresses')}} A on O.id_address = A.id_address
    group by P.id_product, product_name, A.state
)

select * from agregada

-- total de ventas por estado y producto--