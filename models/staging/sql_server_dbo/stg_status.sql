 SELECT 
     distinct({{ dbt_utils.generate_surrogate_key(['status'])}}) as status_id,
     status
 FROM {{ source('sql_server_dbo','orders') }}

union all 

 SELECT 
     distinct({{ dbt_utils.generate_surrogate_key(['status'])}}),
     status
 FROM {{ source('sql_server_dbo','promos') }}

