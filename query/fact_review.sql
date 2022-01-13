select row_number() over (order by r.date, w.id) as id, w.id as weather_id, r.date, r.rating, now() as ingesttime, '{}' as ingestby
from staging.review r 
left join dwh.dim_weather w on r.date=w.date