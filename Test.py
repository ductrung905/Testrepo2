# -*- coding: utf-8 -*-
"""
Created on Wed Apr 13 09:36:32 2022

@author: ThomasNB
"""


import math
import openpyxl
from openpyxl import load_workbook
import sqlalchemy as sa
from sqlalchemy import *
import pickle
import datetime as dt
from datetime import date
 
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
import pandas as pd
import time
import numpy as np
import os
import pyodbc
import pandas as pd
import datetime as dt 
from dateutil.relativedelta import *
import dateutil.relativedelta

business_date= dt.datetime(2022,6,1,0,0,0)

business_date_str = business_date.strftime("%Y%m%d")

card=pd.read_excel('D:/data/Card/Card_'+business_date_str+'.xlsx',dtype= {'CUST_KEY':str,'GROUP':str,
                                                             'GROUP2':str,'MÃ KHÁCH HÀNG':str,'ACCOUNT NUMBER':str,'SỐ THẺ':str})
wo= pd.read_excel('D:/data/Card/WO_CARD.xlsx',dtype= {'CUST KEY':str})
wo['WO']=1

card['DUP']=card['MÃ KHÁCH HÀNG']+card['ACCOUNT NUMBER']
card.drop_duplicates(['DUP'], keep='first',inplace=True)

card=pd.merge(card,wo,left_on='MÃ KHÁCH HÀNG',right_on='CUST KEY',how='left')

card=card.loc[card['WO']!=1]
                 
arr_nhom_no_1 = {1: [-1000000000,9],
               2: [10,90],
               3: [91,180],
               4: [181,360],
               5: [361,100000000]
              }

for bucket, dpd in arr_nhom_no_1.items():
            card.loc[(card['SỐ NGÀY QUÁ HẠN'] >= dpd[0])
                          &(card['SỐ NGÀY QUÁ HẠN'] <= dpd[1]), 'NHOM_NO_NO'] = bucket
      
card['CUST_KEY']=card['MÃ KHÁCH HÀNG']
card['DUNO_ADJ']=card['DƯ NỢ']
card.loc[card['DƯ NỢ']>card['HẠN MỨC TÍN DỤNG'],'DUNO_ADJ']=card['HẠN MỨC TÍN DỤNG']
# card.to_excel('D:/data/Card/test card.xlsx')
def function_to_sql(df,name_report):
    # a= r'mssql+pyodbc://sa:Tpb2021@NGUYENNQB1-PC\SQLEXPRESS:1433/My_database?driver=SQL+Server+Native+Client+11.0'
    a= r'mssql+pyodbc://THOMASNB\SQLEXPRESS/CUSTOMER_LOAN_INFO?driver=SQL+Server+Native+Client+11.0'    
    engine = sa.create_engine(a,echo=False)
    tsql_chunksize =len(df.columns) 
    # cap at 1000 (limit for number of rows inserted by table-value constructor)
    tsql_chunksize = 1000 if tsql_chunksize > 1000 else tsql_chunksize
    
    df.to_sql(name = name_report, con=engine, index=False, if_exists='replace', chunksize=tsql_chunksize,
              dtype= {'CUST_KEY':Unicode(),'GROUP':Unicode(),'GROUP2':Unicode(),'MÃ KHÁCH HÀNG':Unicode()})    
    
function_to_sql(card,'CARD_'+business_date_str)        
