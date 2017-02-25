select count(*) as cancellations,cnclcode
from flighttable 
where cncl=1 
group by cnclcode
order by cancellations asc limit 100;
