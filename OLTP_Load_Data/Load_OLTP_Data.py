#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install faker


# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import create_engine
import pymysql


# In[2]:


faker_ = Faker('en_IN')


# In[3]:


engine = create_engine("mysql+pymysql://{user}:{password}@{host}/{db}"
                       .format(user="root",
                               password="Sikta@2411",
                               host="34.23.187.32",
                               db="UFH_Database"))


# ## CUSTOMER_MASTER table

# In[4]:


customer_attributes= ['Customerid','Name','Address','City','State','Pincode','Update_timestamp']


# In[5]:


states = {'Madhya Pradesh':['Indore','Bhopal','Gwalior','Ujjain','Ratlam'],
          'Maharashtra':['Mumbai','Nagpur','Pune'],
          'Gujarat' : ['Vadodara','Ahmedabad','Surat','Gandhi Nagar'],
          'Karnataka':['Bangalore','Mangalore','Mysore'],
          'Uttar Pradesh':['Noida','Lucknow','Mathura'],
          'Jharkhand' : ['Ranchi','Dhanbad'],
          }


# In[6]:


customer = pd.DataFrame(columns=customer_attributes,index=range(1,1001))
Faker.seed(10)
for i in range(1,1001):
    customer['Customerid'][i]=i
    customer['Name'][i]=faker_.name()
    customer['Address'][i]=faker_.street_address()    
    customer['State'][i]=faker_.random_element(states.keys())
    customer['City'][i]=faker_.random_element(states[customer['State'][i]])
    customer['Pincode'][i]=faker_.postcode()
    customer['Update_timestamp'][i]=faker_.date_time_this_year()


# In[8]:


customer.to_sql('CUSTOMER_MASTER', con = engine, if_exists = 'append', index=False)


# ## PRODUCT_MASTER table

# In[9]:


product_attributes = ['Productid','Productcode','Productname','Sku','Rate','Isactive']


# In[10]:


products = ['Organic India Chyavanprash',
            'Aashirwad Atta',
            'Herbal Strategi Oil',
            'Herbal Strategi Floor Cleaner',
            'Soulfull Ragi Bites Choco Fills',
            'Natureland Puffed Wheat',
            'Natureland Pasta Macroni',
            'Natureland Rice Poha',
            'Pure Sure Organic Chakli',
            'Organic Wellness Chilli Chocolate',
            'Natureland Green Tea',
            'Organic India Tulsi Tummy',
            'Organic Wellness Turmeric Chocolate',
            'Organic Wellness Ginger Tea',
            '24 Mantra Organic Orange Juice',
            '24 Mantra Organic Mango Juice',
            'Ayuda Organics Desi Jaggery',
            '24 Mantra Handpounded Rice',
            '24 Mantra Idly Rice',
            'Natureland Brown Sugar',
            'Natureland Sunflower Oil',
            'Natureland Mustard Oil',
            'Natureland Jaggery Powder',
            'Natureland Red Rice',
            'Natureland Rice Flour',
            'Biotique Bio White Face Wash',
            'Vitro Naturals Aloe Face Wipes',
            'Biotique Bio Honey Gel Face Wash',
            'Biotique Bio Orris Root',
            'Vitro Naturals Aloe Face Wash',
            'BioPure Hand Sanitizer',
            'Soultree Lipgloss Nude Pink',
            'Soultree Lipgloss Rich Earth',
            'Soultree Lipgloss Lush Berry',
            'Soultree Lipstick Iced Plum',
            'Soultree Kajal Pure Black',
            'Soultree Grey Glow Kajal',
            'Ayuda Organics Chhundo (Sun Dried)',
            'Ayuda Organics Raw Honey',
            'Ayuda Organics Homemade Mango Pickle',
            'Ayuda Organics Homemade Chana Methi Pickle',
            'Natureland Garlic Pickle',
            'Natureland Pineapple Jam',
            'Natureland Pineapple Jam',
            'Natureland Honey',
            'Natureland Walnut',
            'Natureland Cashew',
            'Natureland Raisins',
            'Natureland Almonds',
            'Pure Sure Organic Rice Dosa Mix'
           ]


# In[11]:


product = pd.DataFrame(columns = product_attributes, index=range(1,201))
Faker.seed(10)
for i in range(1,51):
    product_name = products[i-1]
    for j in range((i*4)-3,(i*4)+1):        
        product['Productid'][j]=j
        product['Productcode'][j]=faker_.bothify('????-########', letters='ABCDE')
        product['Productname'][j]= product_name
        sku = 0
        if j%4 == 1:
            sku = faker_.random_int(min=1, max=10)
        else:
            sku = int(product['Sku'][j-1][:-2]) + 2
        product['Sku'][j] = str(sku) + 'KG'
        product['Rate'][j]=round(np.random.normal(1000,200))
        product['Isactive'][j]=faker_.boolean()


# In[12]:


print(product)


# In[13]:


product.to_sql('PRODUCT_MASTER', con = engine, if_exists = 'append', index=False)


# ## ORDER_DETAILS table

# In[14]:


order_details_attribute = ['Orderid', 'Customerid', 'Order_status_update_timestamp', 'Order_status']


# In[15]:


order_details = pd.DataFrame(columns=order_details_attribute, index=range(1, 60001))
x=1
j=1
for i in range(1, 20001):
    f=faker_.random_element(customer['Customerid'])
    for k in range (j,j+3):
        order_details['Orderid'][k] = x
        order_details['Customerid'][k]=f

    order_details['Order_status_update_timestamp'][j] = faker_.date_time_this_year()
    order_details['Order_status'][j] = 'Received'
    order_details['Order_status_update_timestamp'][j+1] = order_details['Order_status_update_timestamp'][j] + timedelta(seconds=faker_.random_int(min=1, max=86400))
    order_details['Order_status'][j+1] = 'In_progress'
    order_details['Order_status_update_timestamp'][j+2] = order_details['Order_status_update_timestamp'][j+1] + timedelta(seconds=faker_.random_int(min=1, max=172800))
    order_details['Order_status'][j+2] = 'Delivered'
    j+=3
    x+=1


# In[17]:


print(order_details)


# In[16]:


order_details.to_sql('ORDER_DETAILS', con = engine, if_exists = 'append', index=False)


# ## ORDER_ITEMS table

# In[22]:


order_items_attribute = ['Orderid','Productid','Quantity']


# In[23]:


order_items = pd.DataFrame(columns=order_items_attribute, index = range(1,50001))
for i in range(1,50001):
    order_items['Orderid'][i]=faker_.random_element(order_details['Orderid'])
    order_items['Productid'][i]=faker_.random_element(product['Productid'])
    order_items['Quantity'][i]=faker_.random_int(min=1, max=20)


# In[25]:


order_items = order_items.drop_duplicates(subset=['Orderid', 'Productid'],keep='first')


# In[26]:


order_items.to_sql('ORDER_ITEMS', con = engine, if_exists = 'append', index=False)


