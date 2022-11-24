select distinct
EXTRACT(DATE from order_delivery_timestamp) Dates,
EXTRACT(WEEK from order_delivery_timestamp) Weeks,
EXTRACT(DAYOFWEEK from order_delivery_timestamp) Weekdays,
EXTRACT(MONTH from order_delivery_timestamp) Months,
a.City,
Min(order_delivery_time_seconds) MinDeliveryTime,
max(order_delivery_time_seconds) MaxDeliveryTime,
avg(order_delivery_time_seconds) AvgDeliveryTime,
from `pax-9-366608.ufh_dataset.fact_daily_orders` f
join `pax-9-366608.ufh_dataset.dim_customer` c on f.customerid = c.customerid
join `pax-9-366608.ufh_dataset.dim_address` a on c.address_id = a.address_id
group by 
Dates,
Weeks,
Weekdays,
Months,
City