with
stg_budget as (
    select*
    from {{ref('stg_google_sheets__budget')}}
),

budget as (
    select
        _row,
        quantity,
        id_month,
        T.desc_mes,
        id_product,
        _fivetran_synced
    from stg_budget B
    left join {{ref('dim_tiempo')}} T on B.id_month = T.id_date
)

select * from budget