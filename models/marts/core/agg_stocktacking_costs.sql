with 
order_itmes as (
    select *
    from {{ref('fact_order_items')}}
),

agregada as (
    select
        I.id_order,
        I.id_product,
        P.name AS product_name,
        I.quantity,    
        B.quantity AS inventory_quantity,
        T.desc_mes as month,
        P.price_$ AS product_price,
        M.status AS promo_status,
        M.discount AS promo_discount,
        COALESCE(I.quantity * P.price_$, 0) AS total_order_value,
        COALESCE(I.quantity * (P.price_$ - M.discount), 0) - COALESCE(B.quantity * P.price_$, 0)  AS net_sales

    from order_itmes I
    left join {{ref('fact_budget')}} B on I.id_product = B.id_product
    left join {{ref('dim_products')}} P on I.id_product = P.id_product
    left join {{ref('fact_orders')}} O on I.id_order= O.id_order
    left join {{ref('dim_promos')}} M on M.id_promo = O.id_promo
    left join {{ref('dim_tiempo')}} T on B.id_month = T.id_date
)

select *  from agregada 