{{ config(
    materialized='incremental',
    unique_key= 'user_id',
    on_schema_change='fail'
    ) 
    }}

WITH source AS (
    SELECT * 
    FROM {{ source('sql_server_dbo','users') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}

),

users as (

    select
        user_id as id_user,
        cast(updated_at as timestamp_ltz)as updated_at_utc,
        address_id as id_address,
        last_name,
        cast(created_at as timestamp_ltz)as created_at_utc,
        phone_number,
        decode(total_orders,null,'9999',total_orders) AS total_orders,
        first_name,
        email,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from users
