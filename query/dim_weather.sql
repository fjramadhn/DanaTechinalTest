select row_number() OVER (order by t.date) as id, t.date, t.min, t.max, t.normal_min, t.normal_max, p.precipitation, p.precipitation_normal, now() as ingesttime, '{}' as ingestby
from ods.temperature t
left join ods.precipitation p on t.date = p.date