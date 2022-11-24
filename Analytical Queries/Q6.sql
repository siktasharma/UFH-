select 
customerid,
EXTRACT(DATE from order_received_timestamp) Date,
round(avg(order_amount),2) AS OrderAmount
from `pax-9-366608.ufh_dataset.fact_daily_orders` f
group by 
customerid,Date
