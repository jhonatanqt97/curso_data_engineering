select 
distinct ({{ dbt_utils.generate_surrogate_key(['id_status'])}}) as id_status
from {{ ref('stg_promos') }}

union all

select 
distinct ({{ dbt_utils.generate_surrogate_key(['status'])}}) as id_status
from {{ ref('stg_orders') }}