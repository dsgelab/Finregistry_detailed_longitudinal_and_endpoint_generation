#!/usr/bin/env python
# coding: utf-8

# ## Operations for data update period 2020-2021

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/THL2021_2196_AVOHILMO_TOIMP.csv.finreg_IDsp'
start_time = time.time()
oper = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_20_21.csv'
start_time = time.time()
main20_21 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


# In[ ]:


oper = oper.merge(main20_21, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(oper.shape)
print((oper['TNRO']==oper['FINREGISTRYID']).value_counts())


# In[ ]:


oper = oper[(oper['TNRO']==oper['FINREGISTRYID'])] # removed 3 rows because there was no corresponding avohilmo_ID in main file
del main20_21
gc.collect()


# In[ ]:


oper.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
oper = oper.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
oper['CATEGORY'] = oper['CATEGORY'].apply(lambda x: 'OP'+str(x))
oper['PVM'] = pd.to_datetime(oper['PVM'])
oper['EVENT_YRMNTH'] = oper['PVM'].dt.strftime('%Y-%m')
oper['CODE2'] = np.nan
oper['CODE3'] = np.nan
oper['CODE4'] = np.nan
oper['SOURCE'] = "PRIM_OUT"
oper['ICDVER'] = 10
oper = oper[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
oper.fillna("NA", inplace=True)


# In[ ]:


oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_oper_20_21.csv',index=False)


# ## Operations for a period up to 2020

# In[ ]:


###################################
# for 2011-2016 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_toimenpide.csv.finreg_IDsp'
start_time = time.time()
oper = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_11_16.csv'
start_time = time.time()
main11_16 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: OP0:19	Procedures and interventions (SPAT-classificaiton); 0=main diagnosis, 1:19 side dianoses


# In[ ]:


oper = oper.merge(main11_16, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(oper.shape)
print((oper['TNRO']==oper['FINREGISTRYID']).value_counts())


# In[ ]:


oper = oper[(oper['TNRO']==oper['FINREGISTRYID'])] # removed 48219 (0.029%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main11_16
gc.collect()


# In[ ]:


oper.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


oper = oper.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
oper['CATEGORY'] = oper['CATEGORY'].apply(lambda x: 'OP'+str(x))


# In[ ]:


oper['PVM'] = pd.to_datetime(oper['PVM'])
oper['EVENT_YRMNTH'] = oper['PVM'].dt.strftime('%Y-%m')


# In[ ]:


oper['CODE2'] = np.nan
oper['CODE3'] = np.nan
oper['CODE4'] = np.nan
oper['SOURCE'] = "PRIM_OUT"
oper['ICDVER'] = 10


# In[ ]:


oper = oper[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


oper.fillna("NA", inplace=True)


# In[ ]:


oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_oper_11_16.csv',index=False)


# In[ ]:


###################################
# for 2017-2020 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_17_20_toimenpide.csv.finreg_IDsp'
start_time = time.time()
oper = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_17_20.csv'
start_time = time.time()
main17_20 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: OP0:19	Procedures and interventions (SPAT-classificaiton); 0=main diagnosis, 1:19 side dianoses


# In[ ]:


oper = oper.merge(main17_20, on='AVOHILMO_ID', how='left') 


# In[ ]:


#checks
print(oper.shape)
print((oper['TNRO']==oper['FINREGISTRYID']).value_counts())


# In[ ]:


oper = oper[(oper['TNRO']==oper['FINREGISTRYID'])] # removed 97052 (0.05%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main17_20
gc.collect()


# In[ ]:


oper.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


oper = oper.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
oper['CATEGORY'] = oper['CATEGORY'].apply(lambda x: 'OP'+str(x))


# In[ ]:


oper['PVM'] = pd.to_datetime(oper['PVM'])
oper['EVENT_YRMNTH'] = oper['PVM'].dt.strftime('%Y-%m')


# In[ ]:


oper['CODE2'] = np.nan
oper['CODE3'] = np.nan
oper['CODE4'] = np.nan
oper['SOURCE'] = "PRIM_OUT"
oper['ICDVER'] = 10


# In[ ]:


oper = oper[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


oper.fillna("NA", inplace=True)


# In[ ]:


oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_oper_17_20.csv',index=False)


# ### Remove entries from the year 2020 (which is available and is used from data update)

# In[ ]:


oper=pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_oper_17_20.csv')


# In[ ]:


oper['year']=oper['EVENT_YRMNTH'].apply(lambda x: x[:4])
oper['year']=oper['year'].astype(int)
print(oper.shape)
print(oper['year'].value_counts())


# In[ ]:


oper=oper[oper['year']<2020]
print(oper['year'].value_counts())
del oper['year']
print(oper.shape)


# In[ ]:


oper.fillna("NA", inplace=True)


# In[ ]:


oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_oper_17_19.csv',index=False)
