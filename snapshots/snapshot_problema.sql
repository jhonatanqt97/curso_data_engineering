{% snapshot budget_snapshot4 %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=['first_name','last_name','address_id','phone_number'],
      
        )
}}

select * from {{ ref ('stg_usuarios_ejercicio') }}
where F_CARGA = (select max(F_CARGA) from {{ref ('stg_usuarios_ejercicio') }})

{% endsnapshot %}