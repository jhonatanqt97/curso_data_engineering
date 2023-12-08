with 

intermedia as (
    select *
    from {{ref('int_sessions_events')}}
),

agregacion as (
    select
        U.id_user,
        A.id_address,
        first_name,
        last_name,
        phone_number,
        email,
        I.num_events
    from intermedia I
    left join {{ref('dim_users')}} U on I.id_user = U.id_user
    left join {{ref('dim_addresses')}} A on U.id_address = A.id_address

  
)


SELECT * from agregacion
