with 

source as (

    select * from {{ source('google_sheets', 'budget') }}

),

budget as (

    select
        _row,
        quantity,
        LEFT(REPLACE({{ dbt_date.round_timestamp("month")}}, '-', ''), 8) as id_month,
        month,
        product_id as id_product,
        _fivetran_synced

    from source

)

select * from budget
