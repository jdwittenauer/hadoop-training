select key ,  a.arrdelay, a.elaptime, a.airtime, a.distance, ((a.distance / a.airtime) * 60) as airspeed
from flighttable a
order by airspeed desc limit 20;
