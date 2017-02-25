select key,  a.arrdelay, a.elaptime, a.airtime, a.distance 
from flighttable a
order by a.arrdelay desc limit 20;  