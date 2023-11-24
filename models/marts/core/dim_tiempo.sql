with 
fecha as (
    select *
    from {{ref('stg_fechas')}}
),

tiempo as (

    select
        fecha_forecast,
        id_date,
        anio,
        mes,
        desc_mes,
        id_anio_mes,
        dia_previo,
        anio_semana_dia,
        semana
    from fecha
)

select * from tiempo