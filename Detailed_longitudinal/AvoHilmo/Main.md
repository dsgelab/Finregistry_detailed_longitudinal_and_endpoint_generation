
## Data update files for a period 2020-2021


```python
###################################
# for 2020-2021 period
###################################
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np

```


```python
path = '/data/processed_data/thl_avohilmo/THL2021_2196_AVOHILMO_2020.csv.finreg_IDsp'
start_time = time.time()
a20 = pd.read_csv(path,usecols=['AVOHILMO_ID','TNRO','KAYNTI_ALKOI'])
run_time = time.time()-start_time
print(run_time)

path = '/data/processed_data/thl_avohilmo/THL2021_2196_AVOHILMO_2021.csv.finreg_IDsp'
start_time = time.time()
a21 = pd.read_csv(path,usecols=['AVOHILMO_ID','TNRO','KAYNTI_ALKOI'])
run_time = time.time()-start_time
print(run_time)

```


```python
########################## 2020 #########################
```


```python
a20 = a20.rename(columns = {'TNRO': 'FINREGISTRYID'})

a20['KAYNTI_ALKOI'] = pd.to_datetime(a20['KAYNTI_ALKOI']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt')
a20 = a20.merge(dob, on='FINREGISTRYID', how='left') # add DOB
a20['DOB(YYYY-MM-DD)'] = pd.to_datetime(a20['DOB(YYYY-MM-DD)'])
a20['EVENT_AGE'] = (a20['KAYNTI_ALKOI'] - a20['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
a20.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
a20['PVM'] = a20['KAYNTI_ALKOI']
a20['KAYNTI_ALKOI'] = a20['KAYNTI_ALKOI'].dt.strftime('%Y-%m')
```


```python
a20['INDEX'] = np.arange(a20.shape[0])
a20['INDEX'] = a20['INDEX'].astype(str) + '_avo5'
```


```python
a20.drop(columns=['KAYNTI_ALKOI'], inplace=True)
```


```python
print(a20.shape)
```


```python
# make Event age = death age for events recorded after death
path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
a20 = a20.merge(death, on='FINREGISTRYID', how='left') 
a20['EVENT_AGE_QC'] = a20.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)

#checks
print((a20['EVENT_AGE_x']>a20['EVENT_AGE_y']).value_counts(dropna=False))
print((a20['EVENT_AGE_QC']>a20['EVENT_AGE_y']).value_counts(dropna=False))
print(a20.shape)
a20.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
a20 = a20.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',a20[a20['EVENT_AGE']<0].shape[0])
a20.loc[a20['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
########################## 2021 #########################
```


```python
a21 = a21.rename(columns = {'TNRO': 'FINREGISTRYID'})

a21['KAYNTI_ALKOI'] = pd.to_datetime(a21['KAYNTI_ALKOI']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt')
a21 = a21.merge(dob, on='FINREGISTRYID', how='left') # add DOB
a21['DOB(YYYY-MM-DD)'] = pd.to_datetime(a21['DOB(YYYY-MM-DD)'])
a21['EVENT_AGE'] = (a21['KAYNTI_ALKOI'] - a21['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
a21.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
a21['PVM'] = a21['KAYNTI_ALKOI']
a21['KAYNTI_ALKOI'] = a21['KAYNTI_ALKOI'].dt.strftime('%Y-%m')
```


```python
a21['INDEX'] = np.arange(a21.shape[0])
a21['INDEX'] = a21['INDEX'].astype(str) + '_avo5'
```


```python
a21.drop(columns=['KAYNTI_ALKOI'], inplace=True)
```


```python
print(a21.shape)
```


```python
# make Event age = death age for events recorded after death
path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
a21 = a21.merge(death, on='FINREGISTRYID', how='left') 
a21['EVENT_AGE_QC'] = a21.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)

#checks
print((a21['EVENT_AGE_x']>a21['EVENT_AGE_y']).value_counts(dropna=False))
print((a21['EVENT_AGE_QC']>a21['EVENT_AGE_y']).value_counts(dropna=False))
print(a21.shape)
a21.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
a21 = a21.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',a21[a21['EVENT_AGE']<0].shape[0])
a21.loc[a21['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
#################################################################
```


```python
a20_21 = pd.concat([a20, a21])
print(a20_21.shape[0] == (a20.shape[0]+a21.shape[0]))
a20_21.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_20_21.csv',index=False)
```

## Old files for a period up to 2019


```python
###################################
# for 2011-2016 period
###################################
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_11_12.csv.finreg_IDsp'
start_time = time.time()
a11_12 = pd.read_csv(path,usecols=['AVOHILMO_ID','TNRO','KAYNTI_ALKOI'])
run_time = time.time()-start_time
print(run_time)

path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_13_14.csv.finreg_IDsp'
start_time = time.time()
a13_14 = pd.read_csv(path,usecols=['AVOHILMO_ID','TNRO','KAYNTI_ALKOI'])
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_15_16.csv.finreg_IDsp'
start_time = time.time()
a15_16 = pd.read_csv(path,usecols=['AVOHILMO_ID','TNRO','KAYNTI_ALKOI'])
run_time = time.time()-start_time
print(run_time)


#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
```


```python
########################## 11-12 #########################
a11_12 = a11_12.rename(columns = {'TNRO': 'FINREGISTRYID'})
```


```python
a11_12['KAYNTI_ALKOI'] = pd.to_datetime(a11_12['KAYNTI_ALKOI']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt')
a11_12 = a11_12.merge(dob, on='FINREGISTRYID', how='left') # add DOB
a11_12['DOB(YYYY-MM-DD)'] = pd.to_datetime(a11_12['DOB(YYYY-MM-DD)'])
a11_12['EVENT_AGE'] = (a11_12['KAYNTI_ALKOI'] - a11_12['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
a11_12.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
a11_12['PVM'] = a11_12['KAYNTI_ALKOI']
a11_12['KAYNTI_ALKOI'] = a11_12['KAYNTI_ALKOI'].dt.strftime('%Y-%m')
```


```python
a11_12['INDEX'] = np.arange(a11_12.shape[0])
a11_12['INDEX'] = a11_12['INDEX'].astype(str) + '_avo'
```


```python
a11_12.drop(columns=['KAYNTI_ALKOI'], inplace=True)
```


```python
print(a11_12.shape)
```


```python
# make Event age = death age for events recorded after death
path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
a11_12 = a11_12.merge(death, on='FINREGISTRYID', how='left') 
a11_12['EVENT_AGE_QC'] = a11_12.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)

#checks
print((a11_12['EVENT_AGE_x']>a11_12['EVENT_AGE_y']).value_counts(dropna=False))
print((a11_12['EVENT_AGE_QC']>a11_12['EVENT_AGE_y']).value_counts(dropna=False))
print(a11_12.shape)
a11_12.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
a11_12 = a11_12.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',a11_12[a11_12['EVENT_AGE']<0].shape[0])
a11_12.loc[a11_12['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
########################## 13-14 #########################
a13_14 = a13_14.rename(columns = {'TNRO': 'FINREGISTRYID'})
```


```python
a13_14['KAYNTI_ALKOI'] = pd.to_datetime(a13_14['KAYNTI_ALKOI']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
a13_14 = a13_14.merge(dob, on='FINREGISTRYID', how='left') # add DOB
a13_14['DOB(YYYY-MM-DD)'] = pd.to_datetime(a13_14['DOB(YYYY-MM-DD)'])
a13_14['EVENT_AGE'] = (a13_14['KAYNTI_ALKOI'] - a13_14['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
a13_14.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
a13_14['PVM'] = a13_14['KAYNTI_ALKOI']
a13_14['KAYNTI_ALKOI'] = a13_14['KAYNTI_ALKOI'].dt.strftime('%Y-%m')
a13_14['INDEX'] = np.arange(a13_14.shape[0])
a13_14['INDEX'] = a13_14['INDEX'].astype(str) + '_avo1'
```


```python
a13_14.drop(columns=['KAYNTI_ALKOI'], inplace=True)
print(a13_14.shape)
```


```python
# make Event age = death age for events recorded after death
path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
a13_14 = a13_14.merge(death, on='FINREGISTRYID', how='left') 
a13_14['EVENT_AGE_QC'] = a13_14.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)

#checks
print((a13_14['EVENT_AGE_x']>a13_14['EVENT_AGE_y']).value_counts(dropna=False))
print((a13_14['EVENT_AGE_QC']>a13_14['EVENT_AGE_y']).value_counts(dropna=False))
print(a13_14.shape)
a13_14.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
a13_14 = a13_14.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',a13_14[a13_14['EVENT_AGE']<0].shape[0])
a13_14.loc[a13_14['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
a13_14[a13_14['EVENT_AGE'].isna()]
```


```python
a13_14.shape
```


```python
########################## 15-16 #########################
a15_16 = a15_16.rename(columns = {'TNRO': 'FINREGISTRYID'})
```


```python
a15_16['KAYNTI_ALKOI'] = pd.to_datetime(a15_16['KAYNTI_ALKOI']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
a15_16 = a15_16.merge(dob, on='FINREGISTRYID', how='left') # add DOB
a15_16['DOB(YYYY-MM-DD)'] = pd.to_datetime(a15_16['DOB(YYYY-MM-DD)'])
a15_16['EVENT_AGE'] = (a15_16['KAYNTI_ALKOI'] - a15_16['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
a15_16.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
a15_16['PVM'] = a15_16['KAYNTI_ALKOI']
a15_16['KAYNTI_ALKOI'] = a15_16['KAYNTI_ALKOI'].dt.strftime('%Y-%m')
a15_16['INDEX'] = np.arange(a15_16.shape[0])
a15_16['INDEX'] = a15_16['INDEX'].astype(str) + '_avo2'
a15_16.drop(columns=['KAYNTI_ALKOI'], inplace=True)
print(a15_16.shape)
```


```python
# make Event age = death age for events recorded after death
path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
a15_16 = a15_16.merge(death, on='FINREGISTRYID', how='left') 
a15_16['EVENT_AGE_QC'] = a15_16.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)

#checks
print((a15_16['EVENT_AGE_x']>a15_16['EVENT_AGE_y']).value_counts(dropna=False))
print((a15_16['EVENT_AGE_QC']>a15_16['EVENT_AGE_y']).value_counts(dropna=False))
print(a15_16.shape)
a15_16.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
a15_16 = a15_16.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',a15_16[a15_16['EVENT_AGE']<0].shape[0])
a15_16.loc[a15_16['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
###########################################################
```


```python
a11_16 = pd.concat([a11_12, a13_14, a15_16])
print(a11_16.shape[0] == (a11_12.shape[0]+a13_14.shape[0]+a15_16.shape[0]))
a11_16.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_11_16.csv',index=False)
```


```python
del a11_12
del a13_14
del a15_16
del a11_16
gc.collect()  
```


```python
###################################
# for 2017-2020 period
###################################
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_17_18.csv.finreg_IDsp'
start_time = time.time()
a17_18 = pd.read_csv(path,usecols=['AVOHILMO_ID','TNRO','KAYNTI_ALKOI'])
run_time = time.time()-start_time
print(run_time)

path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_19_20.csv.finreg_IDsp'
start_time = time.time()
a19_20 = pd.read_csv(path,usecols=['AVOHILMO_ID','TNRO','KAYNTI_ALKOI'])
run_time = time.time()-start_time
print(run_time)
```


```python
########################## 17-18 #########################
a17_18 = a17_18.rename(columns = {'TNRO': 'FINREGISTRYID'})
```


```python
a17_18['KAYNTI_ALKOI'] = pd.to_datetime(a17_18['KAYNTI_ALKOI']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
a17_18 = a17_18.merge(dob, on='FINREGISTRYID', how='left') # add DOB
a17_18['DOB(YYYY-MM-DD)'] = pd.to_datetime(a17_18['DOB(YYYY-MM-DD)'])
a17_18['EVENT_AGE'] = (a17_18['KAYNTI_ALKOI'] - a17_18['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
a17_18.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
a17_18['PVM'] = a17_18['KAYNTI_ALKOI']
a17_18['KAYNTI_ALKOI'] = a17_18['KAYNTI_ALKOI'].dt.strftime('%Y-%m')
a17_18['INDEX'] = np.arange(a17_18.shape[0])
a17_18['INDEX'] = a17_18['INDEX'].astype(str) + '_avo3'
a17_18.drop(columns=['KAYNTI_ALKOI'], inplace=True)
print(a17_18.shape)
```


```python
# make Event age = death age for events recorded after death
path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
a17_18 = a17_18.merge(death, on='FINREGISTRYID', how='left') 
a17_18['EVENT_AGE_QC'] = a17_18.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)

#checks
print((a17_18['EVENT_AGE_x']>a17_18['EVENT_AGE_y']).value_counts(dropna=False))
print((a17_18['EVENT_AGE_QC']>a17_18['EVENT_AGE_y']).value_counts(dropna=False))
print(a17_18.shape)
a17_18.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
a17_18 = a17_18.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',a17_18[a17_18['EVENT_AGE']<0].shape[0])
a17_18.loc[a17_18['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
a17_18[a17_18['PVM'].isna()]
```


```python
#check
#df = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diagIDs_notinmain.csv')
#df = df.merge(a17_18, on='AVOHILMO_ID', how='left')
#df[df['EVENT_AGE'].notna()]
```


```python
########################## 19-20 #########################
a19_20 = a19_20.rename(columns = {'TNRO': 'FINREGISTRYID'})
```


```python
a19_20['KAYNTI_ALKOI'] = pd.to_datetime(a19_20['KAYNTI_ALKOI']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
a19_20 = a19_20.merge(dob, on='FINREGISTRYID', how='left') # add DOB
a19_20['DOB(YYYY-MM-DD)'] = pd.to_datetime(a19_20['DOB(YYYY-MM-DD)'])
a19_20['EVENT_AGE'] = (a19_20['KAYNTI_ALKOI'] - a19_20['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
a19_20.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
a19_20['PVM'] = a19_20['KAYNTI_ALKOI']
a19_20['KAYNTI_ALKOI'] = a19_20['KAYNTI_ALKOI'].dt.strftime('%Y-%m')
a19_20['INDEX'] = np.arange(a19_20.shape[0])
a19_20['INDEX'] = a19_20['INDEX'].astype(str) + '_avo4'
a19_20.drop(columns=['KAYNTI_ALKOI'], inplace=True)
print(a19_20.shape)
```


```python
# make Event age = death age for events recorded after death
path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
a19_20 = a19_20.merge(death, on='FINREGISTRYID', how='left') 
a19_20['EVENT_AGE_QC'] = a19_20.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)

#checks
print((a19_20['EVENT_AGE_x']>a19_20['EVENT_AGE_y']).value_counts(dropna=False))
print((a19_20['EVENT_AGE_QC']>a19_20['EVENT_AGE_y']).value_counts(dropna=False))
print(a19_20.shape)
a19_20.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
a19_20 = a19_20.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',a19_20[a19_20['EVENT_AGE']<0].shape[0])
a19_20.loc[a19_20['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
a19_20[a19_20['PVM'].isna()]
```


```python
a17_20 = pd.concat([a17_18, a19_20])
print(a17_20.shape[0] == (a17_18.shape[0]+a19_20.shape[0]))
a17_20.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_17_20.csv',index=False)
```