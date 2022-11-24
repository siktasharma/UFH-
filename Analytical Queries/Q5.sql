select 
a.state,
a.city,
count(distinct orderid) NumberOfOrders
from `pax-9-366608.ufh_dataset.fact_daily_orders` f
join `pax-9-366608.ufh_dataset.dim_customer` c on f.customerid = c.customerid
join `pax-9-366608.ufh_dataset.dim_address` a on c.address_id = a.address_id
group by 
state,city
