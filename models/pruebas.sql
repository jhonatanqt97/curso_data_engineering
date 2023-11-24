select 
    count(distinct(id_user))
from {{ref('stg_users')}}

--#2--

select  
    avg(DATEDIFF(day,delivered_at,created_at_utc)) as promedio
from {{ref('stg_orders')}}
where delivered_at between '2020-01-01' and '2024-01-01'

--#3--
with

order_users as(
select
    id_user,
    count distinct(id_order) as order_number
from {{ref(stg_orders)}}
group by 1
),


resultado as (
select
    case
        when order_number >= 3 then '3'
        else order_number :: varchar
    end as order_number,
    count(distinct id_user) as users
from order_users
)
select * from resultado
group by 1



--#4--

with

sesion_unica as (
    select
        count(distinct(id_session))
    from {{ref('stg_sql_server_dbo_events')}}
),

select * from sesion_unica



