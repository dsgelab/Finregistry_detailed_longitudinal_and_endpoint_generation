#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import gc
import time
import datetime
import numpy as np


path = '/data/processed_data/thl_cancer/cancer_2022-06-23.csv'
start_time = time.time()
df = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)
#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']


# In[ ]:


cancer = pd.DataFrame()
cancer['FINREGISTRYID'] = df['finregistryid']
cancer['SOURCE'] = "CANC"


# In[ ]:


#from datetime import date
import datetime as dt
df['dg_date'] = pd.to_datetime(df['dg_date']) # diagnosis date
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
cancer = cancer.merge(dob, on='FINREGISTRYID', how='left') # df_a.merge(df_b, on='mukey', how='left')

cancer['DOB(YYYY-MM-DD)'] = pd.to_datetime(cancer['DOB(YYYY-MM-DD)'])                             
cancer['EVENT_AGE'] = (df['dg_date'] - cancer['DOB(YYYY-MM-DD)']).dt.days/365.24
cancer.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


cancer['PVM'] = df['dg_date']
cancer['EVENT_YRMNTH'] = df['dg_date'].dt.strftime('%Y-%m')


# In[ ]:


# add "C" at the beggining of topo and normalise length to C and 3 digits
def chtopo(x):
    x = str(x)
    if len(x) == 1:
        x = 'C00' + x
    elif len(x) == 2:
        x = 'C0' + x
    else:
        x = 'C' + x
    return x
df['topo']=df['topo'].apply(chtopo)


# In[ ]:


cancer['CODE1'] = df['topo']
cancer['CODE2'] = df['morpho']
cancer['CODE3'] = df['beh']
cancer['CODE4'] = np.nan
cancer['ICDVER'] = "O3"
cancer['CATEGORY'] = np.nan
cancer['INDEX'] = np.arange(df.shape[0]) # c1$INDEX <- 1:nrow(c1)
cancer['INDEX'] = cancer['INDEX'].astype(str) + '_c'


# In[ ]:


# make Event age = death age for events recorded after death

path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
start_time = time.time()
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
run_time = time.time()-start_time
print(run_time)

cancer_death = cancer.merge(death, on='FINREGISTRYID', how='left') 
print("EVENT_AGE > DEATH_AGE:", cancer_death[cancer_death['EVENT_AGE_y']<cancer_death['EVENT_AGE_x']].shape[0])
cancer_death['EVENT_AGE_QC'] = cancer_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1) #(row['EVENT_AGE_y'].notna()) &


# In[ ]:


#checks
print((cancer_death['EVENT_AGE_x']>cancer_death['EVENT_AGE_y']).value_counts(dropna=False))
print((cancer_death['EVENT_AGE_QC']>cancer_death['EVENT_AGE_y']).value_counts(dropna=False))
print(cancer.shape,cancer_death.shape)


# In[ ]:


cancer_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
cancer_death = cancer_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})


# In[ ]:


# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',cancer_death[cancer_death['EVENT_AGE']<0].shape[0])
#cancer_death.loc[cancer_death['EVENT_AGE']<0,'EVENT_AGE'] = 0


# In[ ]:


cancer_death = cancer_death[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
cancer_death.fillna("NA", inplace=True)


# In[ ]:


cancer_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/cancer_until_2020.csv',index=False)