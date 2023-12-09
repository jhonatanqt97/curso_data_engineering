{{ config(
    materialized='incremental',
    unique_key= 'event_id',
    on_schema_change='fail'
    ) 
    }}

WITH source AS (
    SELECT * 
    FROM {{ source('sql_server_dbo','events') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}

),

events as (

    select
        event_id,
        page_url,
        {{ dbt_utils.generate_surrogate_key(['event_type'])}} as event_type,
        user_id,
        decode(product_id,'','9999', product_id) AS product_id,
        session_id,
        created_at,
        decode(order_id,'','9999', order_id) AS order_id,
        _fivetran_deleted,
        _fivetran_synced

    from source

),

casted_events as (
    select
        event_id as id_event,
        page_url,
        event_type as id_event_type,
        user_id as id_user,
        decode(product_id,'','9999', product_id) AS id_product,
        session_id as id_session,
        cast(created_at as timestamp_ltz)as created_at_utc,
        decode(order_id,'','9999', order_id) AS id_order,
        _fivetran_deleted,
        _fivetran_synced
    from events
)

select * from casted_events
