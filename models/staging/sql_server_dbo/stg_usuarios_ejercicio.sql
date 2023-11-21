{{ config(
    materialized='incremental'
    ) 
    }}


WITH stg_usuarios AS (
    SELECT * 
    FROM {{ source('sql_server_dbo','users') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(F_CARGA) from {{ this }})

{% endif %}
    ),

renamed_casted AS (
    SELECT
          user_id
          ,first_name
          ,last_name
          ,address_id
          ,cast(replace(phone_number  ,'-','')as int)  as phone_number
          ,_fivetran_synced as F_CARGA
    FROM stg_usuarios
    )

SELECT * FROM renamed_casted