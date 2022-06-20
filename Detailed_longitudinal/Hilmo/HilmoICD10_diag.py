#!/usr/bin/env python
# coding: utf-8

# ## Dignoses for data update period 2019-2021

# ### Data cleaning for ICD10 codes

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np

path = '/data/processed_data/thl_hilmo/THL2021_2196_HILMO_DIAG.csv.finreg_IDsp'
start_time = time.time()
diag = pd.read_csv(path,usecols=['HILMO_ID','N','KOODI'])
run_time = time.time()-start_time
print(run_time)


# In[ ]:


diag['len']=diag['KOODI'].apply(lambda x: len(x))


# In[ ]:


diag['len'].value_counts(dropna=False)


# In[ ]:


diag[diag.KOODI.str.contains(r'[*&#+]')] # This issue is corrected forthsis period


# In[ ]:


diag['KOODI'] = diag['KOODI'].map(lambda x: x.lstrip('*&#+').rstrip('*&#+'))


# In[ ]:


diag[diag.KOODI.str.contains(r'[*&#+]')]


# In[ ]:


diag[diag.KOODI.str.contains(r'[.]')]


# In[ ]:


diag['KOODI'] = diag['KOODI'].map(lambda x: x.replace(".", ""))


# In[ ]:


diag[diag.KOODI.str.contains(r'[.]')]


# In[ ]:


del diag['len']


# In[ ]:


# There are no double codes in a data update


# ### main code

# In[ ]:


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main2020_2021.csv'
start_time = time.time()
hilmo = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']


# In[ ]:


diag = diag.merge(hilmo, on='HILMO_ID', how='left') 


# In[ ]:


del hilmo
gc.collect()
diag.drop(columns=['HILMO_ID'], inplace=True)
diag = diag.rename(columns = {'ERIK_AVO': 'SOURCE'})
diag['SOURCE'] = diag['SOURCE'].apply(lambda x: 'OUTPAT' if x == 1 else 'INPAT')
diag = diag.rename(columns = {'N': 'CATEGORY','KOODI': 'CODE1'})
diag['CODE2'] = np.nan
diag['CODE3'] = np.nan


# In[ ]:


diag = diag[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


diag['ICDVER'] = diag['ICDVER'].astype(int)


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


# remove eentires from before 2019
diag['year']=diag['EVENT_YRMNTH'].apply(lambda x: x[:4])
diag['year']=diag['year'].astype(int)
diag=diag[diag['year']>=2019]
del diag['year']


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_in_out_ICD10_2019_2021.csv',index=False)


# ## Dignoses for a period up to 2019

# ### Data cleaning for ICD10 codes

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo_diagnoosit_kaikki.csv.finreg_IDsp'
start_time = time.time()
diag = pd.read_csv(path,usecols=['HILMO_ID','N','KOODI'])
run_time = time.time()-start_time
print(run_time)


# In[ ]:


diag['len']=diag['KOODI'].apply(lambda x: len(x))


# In[ ]:


diag['len'].value_counts(dropna=False)


# In[ ]:


diag[diag.KOODI.str.contains(r'[*&#+]')] # This issue is corrected forthsis period


# In[ ]:


# Some code cleaning


# In[ ]:


diag['KOODI'] = diag['KOODI'].map(lambda x: x.lstrip('*&#+').rstrip('*&#+'))


# In[ ]:


diag[diag.KOODI.str.contains(r'[*&#+]')]


# In[ ]:


diag['KOODI'] = diag['KOODI'].map(lambda x: x.replace("#A", "").replace("#H", "").replace("#N", "").replace("*A", "").replace("*E", "").replace("#G", "").replace("#C", ""))


# In[ ]:


diag[diag.KOODI.str.contains(r'[*&#+]')]


# In[ ]:


diag[diag.KOODI.str.contains(r'[.]')]


# In[ ]:


diag['KOODI'] = diag['KOODI'].map(lambda x: x.replace(".", ""))


# In[ ]:


diag[diag.KOODI.str.contains(r'[.]')]


# In[ ]:


del diag['len']


# In[ ]:


# There are no double codes separated with special caracters in Hilmo


# ### main code

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo_diagnoosit_kaikki.csv.finreg_IDsp'
start_time = time.time()
diag = pd.read_csv(path,usecols=['HILMO_ID','N','KOODI'])
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main.csv'
start_time = time.time()
hilmo = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']


# In[ ]:


diag = diag.merge(hilmo, on='HILMO_ID', how='left') 
del hilmo
gc.collect()
diag.drop(columns=['HILMO_ID'], inplace=True)
diag = diag.rename(columns = {'ERIK_AVO': 'SOURCE'})
diag['SOURCE'] = diag['SOURCE'].apply(lambda x: 'OUTPAT' if x == 1 else 'INPAT')
diag = diag.rename(columns = {'N': 'CATEGORY','KOODI': 'CODE1'})
diag['CODE2'] = np.nan
diag['CODE3'] = np.nan


# In[ ]:


diag = diag[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


diag = diag[diag['PVM'].notna()] # drop NANs 4 rows


# In[ ]:


diag['ICDVER'] = diag['ICDVER'].astype(int)


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_in_out_ICD10.csv',index=False)


# In[ ]:


import pandas as pd


# In[ ]:


diag=pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_in_out_ICD10.csv')


# In[ ]:


# remove eentires from before 2019
diag['year']=diag['EVENT_YRMNTH'].apply(lambda x: x[:4])
diag['year']=diag['year'].astype(int)
diag=diag[diag['year']<2019]
del diag['year']


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_in_out_ICD10_until_2018.csv',index=False)
