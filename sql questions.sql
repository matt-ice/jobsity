with
freq as (
    select region, count(*) as frequency
    from trips
    group by region
    order by count(*) desc
    limit 2
),
src as (
    select datasource, max(datetime) as latest
    from trips
        inner join freq on trips.region = freq.region
    group by datasource
    order by max(datetime)
    limit 1
)
select datasource
from src;


select distinct region
from trips
where datasource = 'cheap_mobile';