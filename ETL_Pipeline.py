import pandas as pd
import numpy as np
from datetime import datetime, timedelta,date
import pandas_gbq
import sqlalchemy
from faker import Faker
from sqlalchemy import create_engine
import pymysql
db = sqlalchemy.create_engine(
     sqlalchemy.engine.url.URL(
          drivername='mysql+pymysql',
          username='root',
          password="Sikta@2411",
          host='34.23.187.32',
          database="UFH_Database"
     )
)
project_id='pax-7-366608'
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(r'C:\\Users\\siktasharma24\\Desktop\\pax-7-366608-e6ee7ed3acac.json')

def dim_address():
    address_cols = ['address_id', 'address', 'city', 'state', 'pincode']
    address = pd.DataFrame(columns=address_cols)
    try:
        qry = ' Select MAX(Customerid) from ufh_dataset.dim_customer '
        max_df1= pd.read_gbq(qry,project_id=project_id,credentials=credentials)
        if pd.isna(max_df1.iat[0,0]):
            max_cus=0
        else:
            max_cus=int(max_df1.iat[0,0])

    except:
        max_cus=0

    query1= 'Select * from CUSTOMER_MASTER where Customerid >{} '.format(max_cus)
    df_customer= pd.read_sql(query1,con=db)
    len1=len(df_customer)

    try :
        qry= ' Select MAX(Address_id) from ufh_dataset.dim_address '
        max_df2= pd.read_gbq(qry,project_id=project_id,credentials=credentials)
        if pd.isna(max_df2.iat[0, 0]):
            max_add = 0
        else:
            max_add = int(max_df2.iat[0, 0])
    except:
        max_add= 0


    address_ins = pd.DataFrame(columns = address_cols,index=range(1,len1+1))
    for i in range(1, len1+ 1):
        address_ins['address_id'][i] = int(max_add) + 1
        address_ins['address'][i] = df_customer['Address'][i-1]
        address_ins['city'][i] = df_customer['City'][i-1]
        address_ins['state'][i] = df_customer['State'][i-1]
        address_ins['pincode'][i] = df_customer['Pincode'][i-1]

        max_add = int(max_add) + 1

    print("a")
    address_ins['address_id']=address_ins["address_id"].astype(int)
    address_ins['pincode'] = address_ins["pincode"].astype(int)

    table= 'ufh_dataset.dim_address'
    pandas_gbq.to_gbq(address_ins, table, project_id=project_id,if_exists='replace',credentials=credentials)



dim_address()


def dim_customer():
    dim_customer_cols = ['customerid', 'name', 'address_id', 'start_date', 'end_date']
    dim_customer = pd.DataFrame(columns=dim_customer_cols)
    try:
        qry = ' Select MAX(Customerid) from ufh_dataset.dim_customer '
        max_df1= pd.read_gbq(qry,project_id=project_id,credentials=credentials)
        if pd.isna(max_df1.iat[0,0]):
            max_cus=0
        else:
            max_cus=int(max_df1.iat[0,0])

    except:
        max_cus=0

    query1 = 'Select * from CUSTOMER_MASTER where Customerid >{} '.format(max_cus)
    df_customer= pd.read_sql(query1,con=db)
    len1 = len(df_customer)


    query2 = 'Select * from ufh_dataset.dim_address '
    address = pd.read_gbq(query2, project_id=project_id, credentials=credentials)
    merged_df = pd.merge(df_customer, address, left_on=['Address','City', 'State', 'Pincode'],right_on=['address', 'city', 'state', 'pincode'], how='left')
    merged_df.drop(columns= ['Address','City','State','Pincode','address',"city","state","pincode"],inplace=True)
    merged_df.rename(columns={"Update_timestamp": 'start_time'}, inplace=True)
    merged_df['start_time']=merged_df['start_time'].astype(np.datetime64)
    merged_df['end_date'] = pd.to_datetime(date(2100, 12, 31), errors='ignore')
    merged_df['end_date']=merged_df['end_date'].astype(np.datetime64)
    print('a')
    table = 'ufh_dataset.dim_customer'
    pandas_gbq.to_gbq(merged_df, table, project_id=project_id, if_exists='replace', credentials=credentials)
    return max_cus

max_cus=dim_customer()


def dim_order():
    try:
        qry = ' Select MAX(Orderid) from ufh_dataset.dim_order '
        max_df1 = pd.read_gbq(qry, project_id=project_id, credentials=credentials)
        if pd.isna(max_df1.iat[0,0]):
            max_order = 0
        else:
            max_order = int(max_df1.iat[0,0])

    except:
        max_order = 0

    query= 'Select * from ORDER_DETAILS where Orderid > {}'.format(max_order)
    df_order=pd.read_sql(query,con=db)
    df_order.drop(columns=['Customerid'])
    table='ufh_dataset.dim_order'
    pandas_gbq.to_gbq(df_order, table, project_id=project_id, if_exists='replace', credentials=credentials)
    return max_order
max_order=dim_order()

def f_order_details(max_order):
    qry1='''select * from ORDER_ITEMS where Orderid in (select distinct Orderid from ORDER_DETAILS where Order_status = 'In_progress' and Orderid>{} )'''.format(max_order)
    order_items=pd.read_sql(qry1,con=db)
    qry2 = 'Select * from ORDER_DETAILS where Orderid > {}'.format(max_order)
    df_order = pd.read_sql(qry2, con=db)
    delivered_order=  df_order.where(df_order['Order_status']=='Delivered').dropna()
    order_details= pd.merge(order_items,delivered_order,on="Orderid")
    order_details.drop(columns=['Customerid','Order_status'],inplace=True)
    order_details.rename(columns={"Order_status_update_timestamp": 'order_delivery_timestamp'}, inplace=True)

    table='ufh_dataset.f_order_details'
    pandas_gbq.to_gbq(order_details, table, project_id=project_id, if_exists='replace', credentials=credentials)
    return order_details

f_order=f_order_details(max_order)

def fact_daily_orders_transform(max_order,max_cus,f_order):
    fields = ['customerid', 'orderid', 'order_received_timestamp', 'order_delivery_timestamp', 'pincode',
              'order_amount', 'item_count', 'order_delivery_time_seconds']
    fact_daily_orders = pd.DataFrame(columns=fields)

    qry= 'Select * from ORDER_DETAILS where Orderid > {}'.format(max_order)
    order_details= pd.read_sql(qry,con=db)
    qry2= 'Select * from CUSTOMER_MASTER where Customerid > {}'.format(max_cus)
    customer_details= pd.read_sql(qry2,con=db)
    qry3='Select * from PRODUCT_MASTER '
    product_details=pd.read_sql(qry3,con=db)


    received_df = order_details.where(order_details['Order_status'] == 'Received').dropna()[
        ['Customerid', 'Orderid', 'Order_status_update_timestamp']]
    delivered_df = order_details.where(order_details['Order_status'] == 'Delivered').dropna()[
        ['Customerid', 'Orderid', 'Order_status_update_timestamp']]
    merged_df = pd.merge(received_df, delivered_df, on=['Customerid', 'Orderid'], how='left')
    merged_df.rename(columns={'Order_status_update_timestamp_x': 'order_received_timestamp',
                            'Order_status_update_timestamp_y': 'order_delivery_timestamp'}, inplace=True)
    merged_df['order_delivery_time_seconds'] = pd.to_datetime(
        merged_df['order_delivery_timestamp']) - pd.to_datetime(merged_df['order_received_timestamp'])
    merged_df['order_delivery_time_seconds'] = pd.to_timedelta(merged_df['order_delivery_time_seconds']).view(np.int64) / 1e9

    merged_df = pd.merge(merged_df, customer_details, on=['Customerid'], how='left')
    merged_df.drop(['Name', 'Address', 'City', 'State', 'Update_timestamp'],inplace=True,axis=1)


    cost_df = pd.merge(f_order, product_details, on='Productid', how='left')
    cost_df['Quantity']= cost_df["Quantity"].map(int)
    cost_df['order_amount'] = cost_df.Quantity *cost_df.Rate
    cost_df.drop(['Productcode', 'Productname', 'Sku', 'Isactive'], inplace=True,axis=1)

    amount_df = cost_df[['Orderid', 'order_amount']]

    amount_df = amount_df.groupby('Orderid').sum()
    amount_df.reset_index(level=0,inplace=True)
    quantity_per_order = cost_df[['Orderid', 'Quantity']]
    quantity_per_order = quantity_per_order.groupby('Orderid').sum()
    quantity_per_order.reset_index(level=0,inplace=True)
    merged_df['Orderid']=merged_df["Orderid"].map(str)
    merged_df["Orderid"]=merged_df['Orderid'].astype(float).astype(int)
    merged_df = pd.merge(merged_df, amount_df, on='Orderid', how='left')
    fact_daily_orders = pd.merge(merged_df, quantity_per_order, on='Orderid', how='left')
    fact_daily_orders.rename(columns={'Quantity': 'item_count'}, inplace=True)
    fact_daily_orders.fillna(0, inplace=True)
    cols=['Customerid',"item_count"]
    for i in cols:
        fact_daily_orders[i]=fact_daily_orders[i].astype(float).astype(int)
    
    table='ufh_dataset.fact_daily_orders'
    pandas_gbq.to_gbq(fact_daily_orders, table, project_id=project_id, if_exists='replace', credentials=credentials)


fact_daily_orders_transform(max_order,max_cus,f_order)

def dim_product():
    try:
        qry = ' Select MAX(Productid) from ufh_dataset.dim_product '
        max_df1= pd.read_gbq(qry,project_id=project_id,credentials=credentials)
        if pd.isna(max_df1.iat[0,0]):
            max_prod=0
        else:
            max_prod=int(max_df1.iat[0,0])

    except:
        max_prod=0
    qry2= 'select * from PRODUCT_MASTER where Productid > {}'.format(max_prod)
    prod_ins=pd.read_sql(qry2,con=db)

    prod_ins['start_date'] = datetime.now().date()
    prod_ins['end_date'] = pd.to_datetime(date(2100, 12, 31), errors='ignore')
    prod_ins['end_date'] = prod_ins['end_date'].astype(np.datetime64)
    prod_ins['start_date'] = prod_ins['start_date'].astype(np.datetime64)

    table='ufh_dataset.dim_product'
    pandas_gbq.to_gbq(prod_ins,table, project_id=project_id, if_exists='replace', credentials=credentials)
dim_product()