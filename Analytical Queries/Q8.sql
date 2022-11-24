select distinct
EXTRACT(DATE from order_received_timestamp) Dates,
count(*) over (partition by EXTRACT(DATE from order_received_timestamp)) CustomerCounts
from `pax-9-366608.ufh_dataset.fact_daily_orders` f
group by 
order_received_timestamp
order by Dates