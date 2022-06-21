#!/usr/bin/env python
# coding: utf-8

# ## ICPC2 for data update period 2020-2021

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/THL2021_2196_AVOHILMO_ICPC2_DIAG.csv.finreg_IDsp'
start_time = time.time()
icp = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_20_21.csv'
start_time = time.time()
main20_21 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


# In[ ]:


icp = icp.merge(main20_21, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(icp.shape)
print((icp['TNRO']==icp['FINREGISTRYID']).value_counts())


# In[ ]:


icp = icp[(icp['TNRO']==icp['FINREGISTRYID'])] # removed 49895 (0.03%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main20_21
gc.collect()


# In[ ]:


icp.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
icp = icp.rename(columns = {'JARJESTYS': 'CATEGORY','ICPC2': 'CODE1'})
icp['CATEGORY'] = icp['CATEGORY'].apply(lambda x: 'ICP'+str(x))


# In[ ]:


icp['PVM'] = pd.to_datetime(icp['PVM'])
icp['EVENT_YRMNTH'] = icp['PVM'].dt.strftime('%Y-%m')


# In[ ]:


icp['EVENT_YRMNTH'].value_counts().index.tolist()


# In[ ]:


icp['CODE2'] = np.nan
icp['CODE3'] = np.nan
icp['CODE4'] = np.nan
icp['SOURCE'] = "PRIM_OUT"
icp['ICDVER'] = 10
icp = icp[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
icp.fillna("NA", inplace=True)


# In[ ]:


icp.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_icpc2_20_21.csv',index=False)


# ## ICPC2 for a period up to 2020

# In[ ]:


###################################
# for 2011-2016 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_icpc2.csv.finreg_IDsp'
start_time = time.time()
icp = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_11_16.csv'
start_time = time.time()
main11_16 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: ICP0:11 	Causes of the visit according to ICPC2 classification; 0=main diagnosis, 1:11 side diagnoses


# In[ ]:


icp = icp.merge(main11_16, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(icp.shape)
print((icp['TNRO']==icp['FINREGISTRYID']).value_counts())


# In[ ]:


icp = icp[(icp['TNRO']==icp['FINREGISTRYID'])] # removed 49895 (0.03%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main11_16
gc.collect()


# In[ ]:


icp.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


icp = icp.rename(columns = {'JARJESTYS': 'CATEGORY','ICPC2': 'CODE1'})


# In[ ]:


icp['CATEGORY'] = icp['CATEGORY'].apply(lambda x: 'ICP'+str(x))


# In[ ]:


icp['PVM'] = pd.to_datetime(icp['PVM'])
icp['EVENT_YRMNTH'] = icp['PVM'].dt.strftime('%Y-%m')


# In[ ]:


icp['CODE2'] = np.nan
icp['CODE3'] = np.nan
icp['CODE4'] = np.nan
icp['SOURCE'] = "PRIM_OUT"
icp['ICDVER'] = 10


# In[ ]:


icp = icp[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


icp.fillna("NA", inplace=True)


# In[ ]:


icp.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_icpc2_11_16.csv',index=False)


# In[ ]:


###################################
# for 2017-2020 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_17_20_icpc2.csv.finreg_IDsp'
start_time = time.time()
icp = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_17_20.csv'
start_time = time.time()
main17_20 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: ICP0:11 	Causes of the visit according to ICPC2 classification; 0=main diagnosis, 1:11 side diagnoses


# In[ ]:


icp = icp.merge(main17_20, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(icp.shape)
print((icp['TNRO']==icp['FINREGISTRYID']).value_counts())


# In[ ]:


icp = icp[(icp['TNRO']==icp['FINREGISTRYID'])] # removed 90958 (0.06%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main17_20
gc.collect()


# In[ ]:


icp.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


icp = icp.rename(columns = {'JARJESTYS': 'CATEGORY','ICPC2': 'CODE1'})


# In[ ]:


icp['CATEGORY'] = icp['CATEGORY'].apply(lambda x: 'ICP'+str(x))


# In[ ]:


icp['PVM'] = pd.to_datetime(icp['PVM'])
icp['EVENT_YRMNTH'] = icp['PVM'].dt.strftime('%Y-%m')


# In[ ]:


icp['CODE2'] = np.nan
icp['CODE3'] = np.nan
icp['CODE4'] = np.nan
icp['SOURCE'] = "PRIM_OUT"
icp['ICDVER'] = 10


# In[ ]:


icp = icp[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


icp.fillna("NA", inplace=True)


# In[ ]:


icp.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_icpc2_17_20.csv',index=False)


# ### Remove entries from the year 2020 (which is available and is used from data update)

# In[ ]:


icp=pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_icpc2_17_20.csv')


# In[ ]:


icp['year']=icp['EVENT_YRMNTH'].apply(lambda x: x[:4])
icp['year']=icp['year'].astype(int)
print(icp.shape)
print(icp['year'].value_counts())


# In[ ]:


icp=icp[icp['year']<2020]
print(icp['year'].value_counts())
del icp['year']
print(icp.shape)


# In[ ]:


icp.fillna("NA", inplace=True)


# In[ ]:


icp.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_icpc2_17_19.csv',index=False)