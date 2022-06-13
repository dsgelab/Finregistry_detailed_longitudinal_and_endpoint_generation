#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


# ## Detailed longitudinal for a period up to 2019

# In[ ]:



path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo.csv.finreg_IDsp'
start_time = time.time()
hilmo = pd.read_csv(path,usecols=['HILMO_ID','TNRO','TUPVA','LPVM','PALA'])
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']


# In[ ]:


hilmo = hilmo.rename(columns = {'TNRO': 'FINREGISTRYID'})
hilmo['TUPVA'] = pd.to_datetime(hilmo['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
hilmo = hilmo.merge(dob, on='FINREGISTRYID', how='left') # add DOB
hilmo['DOB(YYYY-MM-DD)'] = pd.to_datetime(hilmo['DOB(YYYY-MM-DD)'])
hilmo['EVENT_AGE'] = (hilmo['TUPVA'] - hilmo['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
hilmo.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


# Separate Hilmo inpatient and Hilmo outpatient
hilmo['EVENT_YEAR'] = hilmo['TUPVA'].dt.strftime('%Y')
hilmo['EVENT_YEAR'] = pd.to_numeric(hilmo['EVENT_YEAR'] )
hilmo['ERIK_AVO']= np.nan
hilmo['ERIK_AVO'] = hilmo.apply(lambda row: 1 if (row['EVENT_YEAR']>=1998) & (row['PALA']>9) else 0, axis=1) #, axis=1 # hilmo$ERIK_AVO <- as.numeric(hilmo$PALA!=''&as.numeric(hilmo$PALA)>9&hilmo$EVENT_YEAR>=1998)


# In[ ]:


# create detailed longitudinal PVM, EVENT_YRMNTH, ICDVER, INDEX coluns
hilmo['PVM'] = hilmo['TUPVA']
hilmo['EVENT_YRMNTH'] = hilmo['TUPVA'].dt.strftime('%Y-%m')
hilmo['ICDVER'] = 10
hilmo['INDEX'] = np.arange(hilmo.shape[0])
hilmo['INDEX'] = hilmo['INDEX'].astype(str) + '_H_ICD10'


# In[ ]:


# record inpatient stay duration in days
hilmo['LPVM'] = pd.to_datetime(hilmo['LPVM']) # discharge day
hilmo['CODE4'] = (hilmo['LPVM'] - hilmo['TUPVA']).dt.days
# if stay is negative put NAN
hilmo.loc[hilmo['CODE4'] <0, 'CODE4'] = np.nan


# In[ ]:


# remove rows with PVM is NAN
print('PVM is NAN:', hilmo[hilmo['PVM'].isna()].shape[0])
hilmo = hilmo[hilmo['PVM'].notna()] # drop NANs 


# In[ ]:


hilmo.drop(columns=['PALA','TUPVA','LPVM','EVENT_YEAR'], inplace=True)


# In[ ]:


# make Event age = death age for events recorded after death

path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
# this file is created within Detailed_longitudinal/COD.ipynb script
start_time = time.time()
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
run_time = time.time()-start_time
print(run_time)

hilmo_death = hilmo.merge(death, on='FINREGISTRYID', how='left') 
del hilmo
gc.collect()


# In[ ]:


print("EVENT_AGE > DEATH_AGE:", hilmo_death[hilmo_death['EVENT_AGE_y']<hilmo_death['EVENT_AGE_x']].shape[0])
hilmo_death['EVENT_AGE_QC'] = hilmo_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)


# In[ ]:


#checks
print((hilmo_death['EVENT_AGE_x']>hilmo_death['EVENT_AGE_y']).value_counts(dropna=False))
print((hilmo_death['EVENT_AGE_QC']>hilmo_death['EVENT_AGE_y']).value_counts(dropna=False))
print(hilmo_death.shape)


# In[ ]:


hilmo_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
hilmo_death = hilmo_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})


# In[ ]:


# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',hilmo_death[hilmo_death['EVENT_AGE']<0].shape[0])
hilmo_death.loc[hilmo_death['EVENT_AGE']<0,'EVENT_AGE'] = 0


# In[ ]:


hilmo_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main.csv',index=False)


# ## Detailed longitudinal for data update period 2020-2021

# In[ ]:


path = '/data/processed_data/thl_hilmo/THL2021_2196_HILMO_2019_2021.csv.finreg_IDsp'
start_time = time.time()
hilmo = pd.read_csv(path,usecols=['HILMO_ID','TNRO','TUPVA','LPVM','PALA'])
run_time = time.time()-start_time
print(run_time)


# In[ ]:


hilmo = hilmo.rename(columns = {'TNRO': 'FINREGISTRYID'})
hilmo['TUPVA'] = pd.to_datetime(hilmo['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
hilmo = hilmo.merge(dob, on='FINREGISTRYID', how='left') # add DOB
hilmo['DOB(YYYY-MM-DD)'] = pd.to_datetime(hilmo['DOB(YYYY-MM-DD)'])
hilmo['EVENT_AGE'] = (hilmo['TUPVA'] - hilmo['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
hilmo.drop(columns='DOB(YYYY-MM-DD)', inplace=True)


# In[ ]:


# Separate Hilmo inpatient and Hilmo outpatient
hilmo['EVENT_YEAR'] = hilmo['TUPVA'].dt.strftime('%Y')
hilmo['EVENT_YEAR'] = pd.to_numeric(hilmo['EVENT_YEAR'] )
hilmo['ERIK_AVO']= np.nan
hilmo['ERIK_AVO'] = hilmo.apply(lambda row: 1 if (row['EVENT_YEAR']>=1998) & (row['PALA']>9) else 0, axis=1) #, axis=1 # hilmo$ERIK_AVO <- as.numeric(hilmo$PALA!=''&as.numeric(hilmo$PALA)>9&hilmo$EVENT_YEAR>=1998)


# In[ ]:


# create detailed longitudinal PVM, EVENT_YRMNTH, ICDVER, INDEX coluns
hilmo['PVM'] = hilmo['TUPVA']
hilmo['EVENT_YRMNTH'] = hilmo['TUPVA'].dt.strftime('%Y-%m')
hilmo['ICDVER'] = 10
hilmo['INDEX'] = np.arange(hilmo.shape[0])
hilmo['INDEX'] = hilmo['INDEX'].astype(str) + '_H_ICD10_u' # different index for updated period + "_u"


# In[ ]:


# record inpatient stay duration in days
hilmo['LPVM'] = pd.to_datetime(hilmo['LPVM']) # discharge day
hilmo['CODE4'] = (hilmo['LPVM'] - hilmo['TUPVA']).dt.days
# if stay is negative put NAN
hilmo.loc[hilmo['CODE4'] <0, 'CODE4'] = np.nan


# In[ ]:


# remove rows with PVM is NAN
print('PVM is NAN:', hilmo[hilmo['PVM'].isna()].shape[0])
hilmo = hilmo[hilmo['PVM'].notna()] # drop NANs 


# In[ ]:


hilmo.drop(columns=['PALA','TUPVA','LPVM','EVENT_YEAR'], inplace=True)


# In[ ]:


# make Event age = death age for events recorded after death

path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
# this file is created within Detailed_longitudinal/COD.ipynb script
start_time = time.time()
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
run_time = time.time()-start_time
print(run_time)

hilmo_death = hilmo.merge(death, on='FINREGISTRYID', how='left') 
del hilmo
gc.collect()


# In[ ]:


print("EVENT_AGE > DEATH_AGE:", hilmo_death[hilmo_death['EVENT_AGE_y']<hilmo_death['EVENT_AGE_x']].shape[0])
hilmo_death['EVENT_AGE_QC'] = hilmo_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)


# In[ ]:


#checks
print((hilmo_death['EVENT_AGE_x']>hilmo_death['EVENT_AGE_y']).value_counts(dropna=False))
print((hilmo_death['EVENT_AGE_QC']>hilmo_death['EVENT_AGE_y']).value_counts(dropna=False))
print(hilmo_death.shape)


# In[ ]:


hilmo_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
hilmo_death = hilmo_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})


# In[ ]:


# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',hilmo_death[hilmo_death['EVENT_AGE']<0].shape[0])
hilmo_death.loc[hilmo_death['EVENT_AGE']<0,'EVENT_AGE'] = 0


# In[ ]:


hilmo_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main2020_2021.csv',index=False)