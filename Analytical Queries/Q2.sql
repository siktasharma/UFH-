Select 
EXTRACT(DAY from order_received_timestamp) AS Day,
EXTRACT(MONTH from order_received_timestamp) AS Month,
EXTRACT(WEEK from order_received_timestamp) AS Week,
a.state AS State,
a.city AS City,
f.pincode AS Pincode,
avg(order_amount) as avg_sales


From  `pax-9-366608.ufh_dataset.fact_daily_orders` f
left join `pax-9-366608.ufh_dataset.dim_customer` c
  on f.customerid = c.customerid
left join `pax-9-366608.ufh_dataset.dim_address` a
  on c.address_id = a.address_id
group by Day, Month, Week, State, City, Pincode