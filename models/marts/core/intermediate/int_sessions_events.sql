with 

events as (
    select * 
    from {{ref("fact_events")}}
),

agrupcion as (
    select 
        count(id_event) as num_events,
        id_user
        
    from events
    group by id_user
),

intermedia as (
    select *
    from agrupcion
)

select * from intermedia

