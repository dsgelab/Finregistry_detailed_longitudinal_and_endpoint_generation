#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# 1994-1995
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo_9495.csv.finreg_IDsp'
start_time = time.time()
hilmo1 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
v11 = pd.DataFrame()


# In[ ]:


v11['FINREGISTRYID'] = hilmo1['TNRO']


# In[ ]:


v11['SOURCE'] = "INPAT"


# In[ ]:


hilmo1['TUPVA'] = pd.to_datetime(hilmo1['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
v11 = v11.merge(dob, on='FINREGISTRYID', how='left') # add DOB
v11['DOB(YYYY-MM-DD)'] = pd.to_datetime(v11['DOB(YYYY-MM-DD)'])
v11['EVENT_AGE'] = (hilmo1['TUPVA'] - v11['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
v11.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


v11['PVM'] = hilmo1['TUPVA']


# In[ ]:


v11['EVENT_YRMNTH'] = hilmo1['TUPVA'].dt.strftime('%Y-%m')


# In[ ]:


v11['CODE1'] = hilmo1['PDG']
v11['CODE2'] = np.nan
v11['CODE3'] = np.nan
hilmo1['LPVM'] = pd.to_datetime(hilmo1['LPVM']) # DATE OF EVENT
v11['CODE4'] = (hilmo1['LPVM'] - hilmo1['TUPVA']).dt.days


# In[ ]:


v11['ICDVER'] = 9


# In[ ]:


v11['CATEGORY'] = 0


# In[ ]:


v12=v11.copy()
v12['CODE1'] = hilmo1['SDG1']
v12['CATEGORY'] = 1


# In[ ]:


v13=v11.copy()
v13['CODE1'] = hilmo1['SDG2']
v13['CATEGORY'] = 2


# In[ ]:


v1 = pd.concat([v11, v12, v13])


# In[ ]:


v1 = v1[v1['CODE1'].notna()].copy()


# In[ ]:


v1['INDEX'] = np.arange(v1.shape[0])
v1['INDEX'] = v1['INDEX'].astype(str) + '_IN9495'


# In[ ]:


######################
# 87-93
######################
path = '/data/processed_data/thl_hilmo/thl2019_1776_poisto_8793.csv.finreg_IDsp'
start_time = time.time()
hilmo2 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
v21 = pd.DataFrame()


# In[ ]:


v21['FINREGISTRYID'] = hilmo2['TNRO']
v21['SOURCE'] = "INPAT"


# In[ ]:


hilmo2['TUPVA'] = pd.to_datetime(hilmo2['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
v21 = v21.merge(dob, on='FINREGISTRYID', how='left') # add DOB
v21['DOB(YYYY-MM-DD)'] = pd.to_datetime(v21['DOB(YYYY-MM-DD)'])
v21['EVENT_AGE'] = (hilmo2['TUPVA'] - v21['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
v21.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


v21['PVM'] = hilmo2['TUPVA']
v21['EVENT_YRMNTH'] = hilmo2['TUPVA'].dt.strftime('%Y-%m')


# In[ ]:


v21['CODE1'] = hilmo2['PDG']
v21['CODE2'] = np.nan
v21['CODE3'] = np.nan
hilmo2['LPVM'] = pd.to_datetime(hilmo2['LPVM']) # DATE OF EVENT
v21['CODE4'] = (hilmo2['LPVM'] - hilmo2['TUPVA']).dt.days


# In[ ]:


v21['ICDVER'] = 9
v21['CATEGORY'] = 0


# In[ ]:


###########################
v22=v21.copy()
v22['CODE1'] = hilmo2['SDG1']
v22['CATEGORY'] = 1


# In[ ]:


v23=v21.copy()
v23['CODE1'] = hilmo2['SDG2']
v23['CATEGORY'] = 2


# In[ ]:


v24=v21.copy()
v24['CODE1'] = hilmo2['SDG3']
v24['CATEGORY'] = 3


# In[ ]:


v2 = pd.concat([v21, v22, v23, v24])


# In[ ]:


v2 = v2[v2['CODE1'].notna()].copy()


# In[ ]:


v2['INDEX'] = np.arange(v2.shape[0])
v2['INDEX'] = v2['INDEX'].astype(str) + '_IN8793'


# In[ ]:


######################
# 69-85
######################
path = '/data/processed_data/thl_hilmo/thl2019_1776_poisto_6986.csv.finreg_IDsp'
start_time = time.time()
hilmo3 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
v31 = pd.DataFrame()


# In[ ]:


v31['FINREGISTRYID'] = hilmo3['TNRO']
v31['SOURCE'] = "INPAT"


# In[ ]:


hilmo3['TULOPV'] = pd.to_datetime(hilmo3['TULOPV']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
v31 = v31.merge(dob, on='FINREGISTRYID', how='left') # add DOB
v31['DOB(YYYY-MM-DD)'] = pd.to_datetime(v31['DOB(YYYY-MM-DD)'])
v31['EVENT_AGE'] = (hilmo3['TULOPV'] - v31['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
v31.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


v31['PVM'] = hilmo3['TULOPV']
v31['EVENT_YRMNTH'] = hilmo3['TULOPV'].dt.strftime('%Y-%m')


# In[ ]:


v31['CODE1'] = hilmo3['DG1']
v31['CODE2'] = np.nan
v31['CODE3'] = np.nan
hilmo3['LAHTOPV'] = pd.to_datetime(hilmo3['LAHTOPV']) # discharge day
v31['CODE4'] = (hilmo3['LAHTOPV'] - hilmo3['TULOPV']).dt.days


# In[ ]:


v31['ICDVER'] = 8
v31['CATEGORY'] = 0


# In[ ]:


###########################
v32=v31.copy()
v32['CODE1'] = hilmo3['DG2']
v32['CATEGORY'] = 1


# In[ ]:


v33=v31.copy()
v33['CODE1'] = hilmo3['DG3']
v33['CATEGORY'] = 2


# In[ ]:


v34=v31.copy()
v34['CODE1'] = hilmo3['DG4']
v34['CATEGORY'] = 3


# In[ ]:


v3 = pd.concat([v31, v32, v33, v34])


# In[ ]:


v3 = v3[v3['CODE1'].notna()].copy()


# In[ ]:


v3['INDEX'] = np.arange(v3.shape[0])
v3['INDEX'] = v3['INDEX'].astype(str) + '_IN6985'


# In[ ]:


v123 = pd.concat([v1, v2, v3])


# In[ ]:


print(v1.shape[0],v2.shape[0],v3.shape[0])


# In[ ]:


# make Event age = death age for events recorded after death

path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
start_time = time.time()
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
run_time = time.time()-start_time
print(run_time)


# In[ ]:


v123_death = v123.merge(death, on='FINREGISTRYID', how='left') 


# In[ ]:


print("EVENT_AGE > DEATH_AGE:", v123_death[v123_death['EVENT_AGE_y']<v123_death['EVENT_AGE_x']].shape[0])


# In[ ]:


v123_death['EVENT_AGE_QC'] = v123_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)


# In[ ]:


#checks
print((v123_death['EVENT_AGE_x']>v123_death['EVENT_AGE_y']).value_counts(dropna=False))
print((v123_death['EVENT_AGE_QC']>v123_death['EVENT_AGE_y']).value_counts(dropna=False))
print(v123.shape,v123_death.shape)


# In[ ]:


v123_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
v123_death = v123_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
v123_death = v123_death[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',v123_death[v123_death['EVENT_AGE']<0].shape[0])
v123_death.loc[v123_death['EVENT_AGE']<0,'EVENT_AGE'] = 0


# In[ ]:


v123_death.fillna("NA", inplace=True)


# In[ ]:


v123_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_inpat_ICD89.csv',index=False)