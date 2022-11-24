select
p.productname,p.sku,
EXTRACT(DAY from order_delivery_timestamp) Day,
EXTRACT(MONTH from order_delivery_timestamp) Month,
sum(quantity) as units_sold
From  `pax-9-366608.ufh_dataset.dim_product` p 
join `pax-9-366608.ufh_dataset.f_order_details` f
on p.productid = f.productid
group by productname,sku,Day,Month