# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 15:00:09 2020

Fetch_data script is designed to download stock market data and stock fundemental data from Tushare, JoinQuant and other data source.
And the script constructs and updates the local database.

@author: Yaosong Zhan

"""
# In[1] Import package
import tushare as ts
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
pro = ts.pro_api('86751c82b4ec16a65999bbb501c281fa974daa3babba9b206fd75d00')

# In[2] Define functions
def get_daily_data(code, start = None, end = None):
    df=pro.daily(ts_code=code, 
                  adj='qfq', 
                  start_date=start, 
                  end_date=end)
    return df

def get_code():
    codes = pro.stock_basic(list_status='L').ts_code.values
    return codes

def update_sql(db_name, start = None, end = None):
    from datetime import datetime,timedelta
    if start == None:
        query_string = f"select trade_date from {db_name}"
        df = pd.read_sql(query_string,engine)
        start = df.trade_date.unique().max()
    if end == None:    
        end = datetime.date.today()
    for code in get_code():
        data=get_data(code,start,end)
        insert_sql(data,db_name)
    
    print(f'{start}:{end}期间{db_name}数据已成功更新')
    return None


def insert_sql(data,db_name,if_exists='append'):
    engine = create_engine('postgresql+psycopg2://postgres:Zys1994zys@localhost:1106/postgres')
    try:
        data.to_sql(db_name,engine,index=False,if_exists=if_exists)
        print(code+'写入数据库成功')
    except:
        pass

def deduplication(db_name):
    conn = psycopg2.connect(database = "postgres", 
                            user = "postgres", 
                            password = "Zys1994zys", 
                            host = "localhost",
                            port = "1106")
    sql_commandline = "delete from stock_daily_bar as ta where ta.ts_code <> ( select max(tb.ts_code) from stock_daily_bar as tb where ta.amount = tb.amount)"
# In[3] Run


for code in get_code():
    data = get_daily_data(code, start = '20190101', end = '20191231')
    insert_sql(data,'stock_daily_bar')
