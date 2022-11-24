select 
EXTRACT(DATE from order_received_timestamp) AS Day,
o.productid,
a.city,
sum(order_amount) total_sales
from `pax-9-366608.ufh_dataset.fact_daily_orders` f
join `pax-9-366608.ufh_dataset.f_order_details` o on f.orderid = o.orderid 
join `pax-9-366608.ufh_dataset.dim_customer` c on f.customerid = c.customerid
join `pax-9-366608.ufh_dataset.dim_address` a on c.address_id = a.address_id
group by 
Day,
productid,
city
order by Day
