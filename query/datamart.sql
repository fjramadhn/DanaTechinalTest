select r.id, r.date, r.rating, w.min, w.max, w.precipitation
from dwh.fact_review r
left join dwh.dim_weather w on r.weather_id = w.id
where r.date >= (now() - interval '1 YEAR');