#!/usr/bin/env python
# coding: utf-8

# ## Mouth procedures for data update period 2020-2021

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2021_2196_avohilmo_suu_toimp.csv.finreg_IDsp'
start_time = time.time()
mouth = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_20_21.csv'
start_time = time.time()
main20_21 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


# In[ ]:


mouth.drop(columns=['TOIMENPIDE_HAMMAS'], inplace=True)
mouth = mouth.merge(main20_21, on='AVOHILMO_ID', how='left') 
#checks
print(mouth.shape)
print((mouth['TNRO']==mouth['FINREGISTRYID']).value_counts())


# In[ ]:


mouth = mouth[(mouth['TNRO']==mouth['FINREGISTRYID'])] # removed 1 row because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main20_21
gc.collect()


# In[ ]:


mouth.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
mouth = mouth.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
mouth['CATEGORY'] = mouth['CATEGORY'].apply(lambda x: 'MOP'+str(x))
mouth['PVM'] = pd.to_datetime(mouth['PVM'])
mouth['EVENT_YRMNTH'] = mouth['PVM'].dt.strftime('%Y-%m')
mouth['CODE2'] = np.nan
mouth['CODE3'] = np.nan
mouth['CODE4'] = np.nan
mouth['SOURCE'] = "PRIM_OUT"
mouth['ICDVER'] = 10
mouth = mouth[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
mouth.fillna("NA", inplace=True)


# In[ ]:


mouth.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_mouth_20_21.csv',index=False)


# ## Mouth procedures for a period up to 2020

# In[ ]:


###################################
# for 2011-2016 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_suu_toimenpide.csv.finreg_IDsp'
start_time = time.time()
mouth = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_11_16.csv'
start_time = time.time()
main11_16 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: MOP0:19+24,26,29	Dental health care measures (NIHW classification of measures); 0=main diagnosis, 1:N side diagnoses


# In[ ]:


mouth.drop(columns=['TOIMENPIDE_HAMMAS'], inplace=True)


# In[ ]:


mouth = mouth.merge(main11_16, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(mouth.shape)
print((mouth['TNRO']==mouth['FINREGISTRYID']).value_counts())


# In[ ]:


mouth = mouth[(mouth['TNRO']==mouth['FINREGISTRYID'])] # removed 25531 (0.035%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main11_16
gc.collect()


# In[ ]:


mouth.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


mouth = mouth.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
mouth['CATEGORY'] = mouth['CATEGORY'].apply(lambda x: 'MOP'+str(x))


# In[ ]:


mouth['PVM'] = pd.to_datetime(mouth['PVM'])
mouth['EVENT_YRMNTH'] = mouth['PVM'].dt.strftime('%Y-%m')


# In[ ]:


mouth['CODE2'] = np.nan
mouth['CODE3'] = np.nan
mouth['CODE4'] = np.nan
mouth['SOURCE'] = "PRIM_OUT"
mouth['ICDVER'] = 10


# In[ ]:


mouth = mouth[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


mouth.fillna("NA", inplace=True)


# In[ ]:


mouth.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_mouth_11_16.csv',index=False)


# In[ ]:


###################################
# for 2017-2020 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_17_20_suu_toimp.csv.finreg_IDsp'
start_time = time.time()
mouth = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_17_20.csv'
start_time = time.time()
main17_20 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: MOP0:19+24,26,29	Dental health care measures (NIHW classification of measures); 0=main diagnosis, 1:N side diagnoses


# In[ ]:


mouth.drop(columns=['TOIMENPIDE_HAMMAS'], inplace=True)


# In[ ]:


mouth = mouth.merge(main17_20, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(mouth.shape)
print((mouth['TNRO']==mouth['FINREGISTRYID']).value_counts())


# In[ ]:


mouth = mouth[(mouth['TNRO']==mouth['FINREGISTRYID'])] # removed 66238 (0.19%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main17_20
gc.collect()


# In[ ]:


mouth.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


mouth = mouth.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
mouth['CATEGORY'] = mouth['CATEGORY'].apply(lambda x: 'MOP'+str(x))


# In[ ]:


mouth['PVM'] = pd.to_datetime(mouth['PVM'])
mouth['EVENT_YRMNTH'] = mouth['PVM'].dt.strftime('%Y-%m')


# In[ ]:


mouth['CODE2'] = np.nan
mouth['CODE3'] = np.nan
mouth['CODE4'] = np.nan
mouth['SOURCE'] = "PRIM_OUT"
mouth['ICDVER'] = 10


# In[ ]:


mouth = mouth[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


mouth.fillna("NA", inplace=True)


# In[ ]:


mouth.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_mouth_17_20.csv',index=False)


# ### Remove entries from the year 2020 (which is available and is used from data update)

# In[ ]:


mouth=pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_mouth_17_20.csv')


# In[ ]:


mouth['year']=mouth['EVENT_YRMNTH'].apply(lambda x: x[:4])
mouth['year']=mouth['year'].astype(int)
print(mouth.shape)
print(mouth['year'].value_counts())


# In[ ]:


mouth=mouth[mouth['year']<2020]
print(mouth['year'].value_counts())
del mouth['year']
print(mouth.shape)


# In[ ]:


mouth.fillna("NA", inplace=True)


# In[ ]:


mouth.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_mouth_17_19.csv',index=False)