select 
Start_time,
count(customerid) NewCustomers
from `pax-9-366608.ufh_dataset.dim_customer`
where customerid in (select customerid from `pax-3-366517.ufh_dataset.dim_customer`
group by customerid having count(*)=1)
group by Start_time
