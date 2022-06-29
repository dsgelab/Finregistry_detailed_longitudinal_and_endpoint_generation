#!/usr/bin/env python
# coding: utf-8

# ## Data update files for a period up to 2020

# In[ ]:


import pandas as pd
import gc
import time
import datetime
import numpy as np
import datetime as dt

path = '/data/processed_data/sf_death/thl2021_2196_ksyy_tutkimus.csv.finreg_IDsp'
start_time = time.time()
df = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)


# In[ ]:


#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
COD1 = pd.DataFrame()


# In[ ]:


COD1['FINREGISTRYID'] = df['TNRO']
COD1['SOURCE'] = "DEATH"
COD1['PVM'] = df['KPV']


# In[ ]:


# USe KVUOSI (year) where KPV (full date) is not available 
def change(x):
    x = str(x) + '-07-01'
    return x

COD1['PVM2'] = df['KVUOSI'].apply(change)
#Joined['DOB(YYYY-MM-DD)_2'] = Joined['DOB(YYYY-MM-DD)']

COD1.loc[COD1['PVM'].isna(),'PVM'] = COD1[COD1['PVM'].isna()]['PVM2']
COD1.drop(columns='PVM2', inplace=True)


# In[ ]:


#from datetime import date

# EVENT_AGE <- as.numeric(as.Date(hilmo4$tulopvm,"%Y-%m-%d") - as.Date(hilmo4$SYNTPVM,"%Y-%m-%d"))/365.24
#cancer['EVENT_AGE'] = date(df['dg_date']) - date(df['bi_date'])
COD1['PVM'] = pd.to_datetime(COD1['PVM']) # diagnosis date
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt')
COD1 = COD1.merge(dob, on='FINREGISTRYID', how='left') # df_a.merge(df_b, on='mukey', how='left')

COD1['DOB(YYYY-MM-DD)'] = pd.to_datetime(COD1['DOB(YYYY-MM-DD)'])                             
COD1['EVENT_AGE'] = (COD1['PVM'] - COD1['DOB(YYYY-MM-DD)']).dt.days/365.24
COD1.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


COD1['EVENT_YRMNTH'] = COD1['PVM'].dt.strftime('%Y-%m')
COD1['CODE1'] = df['TPKS']
COD1['CODE2'] = np.nan
COD1['CODE3'] = np.nan
COD1['CODE4'] = np.nan


# In[ ]:


## 'ICDVER'
COD1['ICDVER'] = pd.to_datetime(COD1['PVM'], format='%Y-%m-%d')#errors='coerce')
COD1['ICDVER'] = COD1['ICDVER'].dt.strftime('%Y')

def isNaN(string):
    return string != string

def icd(x):
    if isNaN(x):
        x = x
    elif int(x) < 1987:
        x = 8
    elif int(x) < 1996:
        x = 9
    elif int(x) >= 1996:
        x = 10
    else:
        print('ERROR',x)
    return x

COD1['ICDVER'] = COD1['ICDVER'].apply(icd)
COD1['ICDVER'] = COD1['ICDVER'].astype(int)


# In[ ]:


COD1['CATEGORY'] = "U"
COD1['INDEX'] = np.arange(df.shape[0])


# In[ ]:


COD1[COD1['EVENT_AGE']<0] # no deaths before DOB


# In[ ]:


COD1 = COD1[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


COD2 = COD1.copy()
COD2['CODE1'] = df['VKS']
COD2['CATEGORY'] = "I"


# In[ ]:


COD3 = COD1.copy()
COD3['CODE1'] = df['M1']
COD3['CATEGORY'] = "c1"


# In[ ]:


COD4 = COD1.copy()
COD4['CODE1'] = df['M2']
COD4['CATEGORY'] = "c2"


# In[ ]:


COD5 = COD1.copy()
COD5['CODE1'] = df['M3']
COD5['CATEGORY'] = "c3"


# In[ ]:


COD6 = COD1.copy()
COD6['CODE1'] = df['M4']
COD6['CATEGORY'] = "c4"


# In[ ]:


COD = pd.concat([COD1, COD2, COD3, COD4, COD5, COD6])
COD = COD[COD['CODE1'].notna()]
COD.fillna("NA", inplace=True)


# In[ ]:


COD.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/COD_until_2020.csv',index=False)


# In[ ]:


COD1.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv',index=False)