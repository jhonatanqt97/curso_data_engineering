{{ config(materialized="incremental", unique_key="_row") }}


with
stg_budget_products as (
    select *
    from {{ source("google_sheets", "budget") }}
    {% if is_incremental() %}

        where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

    {% endif %}
),

renamed_casted as (
    select
        _row,
        month,
        quantity,
        _fivetran_synced
    from stg_budget_products
)

select *
from renamed_casted
