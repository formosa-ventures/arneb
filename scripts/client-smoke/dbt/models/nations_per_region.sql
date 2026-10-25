{{ config(materialized='ephemeral') }}
select r.r_name, count(*) as nations
from {{ source('tpch', 'nation') }} n
join {{ source('tpch', 'region') }} r on n.n_regionkey = r.r_regionkey
group by r.r_name
