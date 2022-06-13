#!/usr/bin/env python
# coding: utf-8

# In[ ]:


######################
# 87-93, codes TMP1,TMP2 
######################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/thl2019_1776_poisto_8793.csv.finreg_IDsp'
start_time = time.time()
hilmo1 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
o6 = pd.DataFrame()


# In[ ]:


o6['FINREGISTRYID'] = hilmo1['TNRO']
o6['SOURCE'] = "OPER_IN"


# In[ ]:


hilmo1['TUPVA'] = pd.to_datetime(hilmo1['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
o6 = o6.merge(dob, on='FINREGISTRYID', how='left') # add DOB
o6['DOB(YYYY-MM-DD)'] = pd.to_datetime(o6['DOB(YYYY-MM-DD)'])
o6['EVENT_AGE'] = (hilmo1['TUPVA'] - o6['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
o6.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


o6['PVM'] = hilmo1['TUPVA']
o6['EVENT_YRMNTH'] = hilmo1['TUPVA'].dt.strftime('%Y-%m')
o6['CODE1'] = hilmo1['TMP1']
o6['CODE2'] = np.nan
o6['CODE3'] = np.nan
o6['CODE4'] = np.nan


# In[ ]:


o6['ICDVER'] = 9
o6['CATEGORY'] = "MFHL1"


# In[ ]:


o7 = o6.copy()
o7['CODE1'] = hilmo1['TMP2']
o7['CATEGORY'] = "MFHL2"


# In[ ]:


o6 = o6[o6['CODE1'].notna()].copy()
o6['INDEX'] = np.arange(o6.shape[0])
o6['INDEX'] = o6['INDEX'].astype(str) + 'o6'
o7 = o7[o7['CODE1'].notna()].copy()
o7['INDEX'] = np.arange(o7.shape[0])
o7['INDEX'] = o7['INDEX'].astype(str) + 'o7'


# In[ ]:


######################
# 94-95, codes TMP1,TMP2,TMP3
######################


path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo_9495.csv.finreg_IDsp'
start_time = time.time()
hilmo2 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
o61 = pd.DataFrame()


# In[ ]:


o61['FINREGISTRYID'] = hilmo2['TNRO']
o61['SOURCE'] = "OPER_IN"


# In[ ]:


hilmo2['TUPVA'] = pd.to_datetime(hilmo2['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
o61 = o61.merge(dob, on='FINREGISTRYID', how='left') # add DOB
o61['DOB(YYYY-MM-DD)'] = pd.to_datetime(o61['DOB(YYYY-MM-DD)'])
o61['EVENT_AGE'] = (hilmo2['TUPVA'] - o61['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
o61.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


o61['PVM'] = hilmo2['TUPVA']
o61['EVENT_YRMNTH'] = hilmo2['TUPVA'].dt.strftime('%Y-%m')
o61['CODE1'] = hilmo2['TMP1']
o61['CODE2'] = np.nan
o61['CODE3'] = np.nan
o61['CODE4'] = np.nan


# In[ ]:


o61['ICDVER'] = 9
o61['CATEGORY'] = "MFHL1"


# In[ ]:


o71 = o61.copy()
o71['CODE1'] = hilmo2['TMP2']
o71['CATEGORY'] = "MFHL2"


# In[ ]:


o81 = o61.copy()
o81['CODE1'] = hilmo2['TMP3']
o81['CATEGORY'] = "MFHL3"


# In[ ]:


o61 = o61[o61['CODE1'].notna()].copy()
o61['INDEX'] = np.arange(o61.shape[0])
o61['INDEX'] = o61['INDEX'].astype(str) + 'o61'
o71 = o71[o71['CODE1'].notna()].copy()
o71['INDEX'] = np.arange(o71.shape[0])
o71['INDEX'] = o71['INDEX'].astype(str) + 'o71'
o81 = o81[o81['CODE1'].notna()].copy()
o81['INDEX'] = np.arange(o81.shape[0])
o81['INDEX'] = o81['INDEX'].astype(str) + 'o81'


# In[ ]:


######################
# 69-85, codes TP1, TP2
######################
path = '/data/processed_data/thl_hilmo/thl2019_1776_poisto_6986.csv.finreg_IDsp'
start_time = time.time()
hilmo3 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
o9 = pd.DataFrame()


# In[ ]:


o9['FINREGISTRYID'] = hilmo3['TNRO']
o9['SOURCE'] = "OPER_IN"


# In[ ]:


hilmo3['TULOPV'] = pd.to_datetime(hilmo3['TULOPV']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
o9 = o9.merge(dob, on='FINREGISTRYID', how='left') # add DOB
o9['DOB(YYYY-MM-DD)'] = pd.to_datetime(o9['DOB(YYYY-MM-DD)'])
o9['EVENT_AGE'] = (hilmo3['TULOPV'] - o9['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
o9.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


o9['PVM'] = hilmo3['TULOPV']
o9['EVENT_YRMNTH'] = hilmo3['TULOPV'].dt.strftime('%Y-%m')
o9['CODE1'] = hilmo3['TP1']
o9['CODE2'] = np.nan
o9['CODE3'] = np.nan
o9['CODE4'] = np.nan


# In[ ]:


o9['ICDVER'] = 8
o9['CATEGORY'] = "SFHL1"


# In[ ]:


o10 = o9.copy()
o10['CODE1'] = hilmo3['TP2']
o10['CATEGORY'] = "SFHL2"


# In[ ]:


o9 = o9[o9['CODE1'].notna()].copy()
o9['INDEX'] = np.arange(o9.shape[0])
o9['INDEX'] = o9['INDEX'].astype(str) + 'o9'
o10 = o10[o10['CODE1'].notna()].copy()
o10['INDEX'] = np.arange(o10.shape[0])
o10['INDEX'] = o10['INDEX'].astype(str) + 'o10'


# In[ ]:


######################
# 94-95, cardio codes TMPTYP1. TMPTYP1, TMPTYP3
######################
path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo_9495_syp.csv.finreg_IDsp'
start_time = time.time()
hilmo4 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
o11 = pd.DataFrame()


# In[ ]:


hilmo5 = hilmo2.merge(hilmo4, on='HILMO_ID', how='left') # add DOB


# In[ ]:


o11['FINREGISTRYID'] = hilmo5['TNRO_x']
o11['SOURCE'] = "OPER_IN"


# In[ ]:


hilmo5['TUPVA'] = pd.to_datetime(hilmo5['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
o11 = o11.merge(dob, on='FINREGISTRYID', how='left') # add DOB
o11['DOB(YYYY-MM-DD)'] = pd.to_datetime(o11['DOB(YYYY-MM-DD)'])
o11['EVENT_AGE'] = (hilmo5['TUPVA'] - o11['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
o11.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


o11['PVM'] = hilmo5['TUPVA']
o11['EVENT_YRMNTH'] = hilmo5['TUPVA'].dt.strftime('%Y-%m')
o11['CODE1'] = hilmo5['TMPTYP1']
o11['CODE2'] = np.nan
o11['CODE3'] = np.nan
o11['CODE4'] = np.nan


# In[ ]:


o11['ICDVER'] = 9
o11['CATEGORY'] = "HPO1"


# In[ ]:


o12 = o11.copy()
o12['CODE1'] = hilmo5['TMPTYP2']
o12['CATEGORY'] = "HPO2"


# In[ ]:


o13 = o11.copy()
o13['CODE1'] = hilmo5['TMPTYP3']
o13['CATEGORY'] = "HPO3"


# In[ ]:


o11 = o11[o11['CODE1'].notna()].copy()
o11['INDEX'] = np.arange(o11.shape[0])
o11['INDEX'] = o11['INDEX'].astype(str) + 'o11'
o12 = o12[o12['CODE1'].notna()].copy()
o12['INDEX'] = np.arange(o12.shape[0])
o12['INDEX'] = o12['INDEX'].astype(str) + 'o12'
o13 = o13[o13['CODE1'].notna()].copy()
o13['INDEX'] = np.arange(o13.shape[0])
o13['INDEX'] = o13['INDEX'].astype(str) + 'o13'


# In[ ]:





# In[ ]:


oper = pd.concat([o6, o7, o61, o71, o81, o9, o10, o11, o12, o13])


# In[ ]:


o6.shape[0]+o7.shape[0]+o61.shape[0]+o71.shape[0]+o81.shape[0]+o9.shape[0]+o10.shape[0]+o11.shape[0]+o12.shape[0]+o13.shape[0]


# In[ ]:


oper.shape


# In[ ]:


# make Event age = death age for events recorded after death

path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
start_time = time.time()
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
run_time = time.time()-start_time
print(run_time)


# In[ ]:


oper_death = oper.merge(death, on='FINREGISTRYID', how='left') 


# In[ ]:


print("EVENT_AGE > DEATH_AGE:", oper_death[oper_death['EVENT_AGE_y']<oper_death['EVENT_AGE_x']].shape[0])


# In[ ]:


oper_death['EVENT_AGE_QC'] = oper_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)


# In[ ]:


#checks
print((oper_death['EVENT_AGE_x']>oper_death['EVENT_AGE_y']).value_counts(dropna=False))
print((oper_death['EVENT_AGE_QC']>oper_death['EVENT_AGE_y']).value_counts(dropna=False))
print(oper.shape,oper_death.shape)


# In[ ]:


oper_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
oper_death = oper_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
oper_death = oper_death[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',oper_death[oper_death['EVENT_AGE']<0].shape[0])
#oper_death.loc[oper_death['EVENT_AGE']<0,'EVENT_AGE'] = 0


# In[ ]:


oper_death.fillna("NA", inplace=True)


# In[ ]:


oper_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_oper_ICD89.csv',index=False)


