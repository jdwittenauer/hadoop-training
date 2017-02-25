select arrdelay,key 
from flighttable 
where arrdelay > 1000
order by arrdelay desc limit 10;

