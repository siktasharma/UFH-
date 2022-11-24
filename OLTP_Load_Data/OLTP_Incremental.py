#!/usr/bin/env python
# coding: utf-8

# In[72]:


import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime, timedelta


# In[73]:


from faker import Faker
fake = Faker('en_IN')


# In[74]:


import sqlalchemy, pymysql
from sqlalchemy.dialects import mysql
import pandas as pd
db = sqlalchemy.create_engine(
     sqlalchemy.engine.url.URL(
          drivername='mysql+pymysql',
          username='root',
          password="Sikta@2411",
          host='34.66.145.88',
          database="UFH_Database"
     )
)


# In[123]:


qry1='select max(customerid) as maxcustid from CUSTOMER_MASTER;'
maxcustid= pd.read_sql(qry1,con=db)
maxcustid=maxcustid.iat[0,0]


# In[124]:


qry2='select max(Productid) from PRODUCT_MASTER;'
maxprodid= pd.read_sql(qry2,con=db)
maxprodid=maxprodid.iat[0,0]


# In[125]:


qry3='select max(orderid) from ORDER_DETAILS;'
orderid= pd.read_sql(qry3,con=db)
orderid=orderid.iat[0,0]
print(orderid)


# # incremental order details

# In[126]:


order_detail_fields = ['orderid', 'customerid', 'order_status_update_timestamp', 'order_status']
orders_Received = pd.DataFrame(columns=order_detail_fields, index=range(1, 15001))

x=orderid+1
j=1
for i in range(1, 5001):
    f=fake.random_int(min=1, max=maxcustid)
    for k in range (j,j+3):
        orders_Received['orderid'][k] = x
        orders_Received['customerid'][k]=f

    orders_Received['order_status_update_timestamp'][j] = fake.date_time_this_year()
    orders_Received['order_status'][j] = 'Received'
    orders_Received['order_status_update_timestamp'][j+1] = orders_Received['order_status_update_timestamp'][j] + timedelta(seconds=fake.random_int(min=1, max=86400))
    orders_Received['order_status'][j+1] = 'In_progress'
    orders_Received['order_status_update_timestamp'][j+2] = orders_Received['order_status_update_timestamp'][j+1] + timedelta(seconds=fake.random_int(min=1, max=172800))
    orders_Received['order_status'][j+2] = 'Delivered'
    j+=3
    x+=1


# In[127]:


orders_Received.head(-10)


# ## loading

# In[128]:


orders_Received.to_sql('ORDER_DETAILS', con=db, if_exists='append',index=False)


# In[ ]:





# # incremental order items

# In[129]:


order_item_fields = ['orderid','productid','quantity']


# In[130]:


order_items = pd.DataFrame(columns=order_item_fields, index = range(1,5001))


# In[131]:


for i in range(1,5001):
    order_items['orderid'][i]=fake.random_element(orders_Received['orderid'])
    order_items['productid'][i]=fake.random_int(min=1, max=maxprodid)
    order_items['quantity'][i]=fake.random_int(min=1, max=20)


# In[132]:


order_items = order_items.drop_duplicates(subset=['orderid', 'productid'],keep='first')


# In[133]:


order_items.head(-10)


# ## loading

# In[134]:


order_items.to_sql('ORDER_ITEMS', con=db, if_exists='append',index=False)

