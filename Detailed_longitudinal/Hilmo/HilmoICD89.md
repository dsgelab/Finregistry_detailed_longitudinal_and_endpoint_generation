

```python
# 1994-1995
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/hilmo/thl2019_1776_hilmo_9495.csv.finreg_IDsp'
start_time = time.time()
hilmo1 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
v1 = pd.DataFrame()
```


```python
v1['FINREGISTRYID'] = hilmo1['TNRO']
```


```python
v1['SOURCE'] = "INPAT"
```


```python
hilmo1['TUPVA'] = pd.to_datetime(hilmo1['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
v1 = v1.merge(dob, on='FINREGISTRYID', how='left') # add DOB
v1['DOB(YYYY-MM-DD)'] = pd.to_datetime(v1['DOB(YYYY-MM-DD)'])
v1['EVENT_AGE'] = (hilmo1['TUPVA'] - v1['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
v1.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
v1['PVM'] = hilmo1['TUPVA']
```


```python
v1['EVENT_YRMNTH'] = hilmo1['TUPVA'].dt.strftime('%Y-%m')
```


```python
v1['CODE1'] = hilmo1['PDG']
v1['CODE2'] = hilmo1['SDG1']
v1['CODE3'] = np.nan
hilmo1['LPVM'] = pd.to_datetime(hilmo1['LPVM']) # DATE OF EVENT
v1['CODE4'] = (hilmo1['LPVM'] - hilmo1['TUPVA']).dt.days
```


```python
v1['ICDVER'] = 9
```


```python
v1['CATEGORY'] = 0
```


```python
v1 = v1[v1['CODE1'].notna()].copy()
```


```python
v1['INDEX'] = np.arange(v1.shape[0])
v1['INDEX'] = v1['INDEX'].astype(str) + '_IN9495'
```


```python
######################
# 87-93
######################
path = '/data/processed_data/thl_hilmo/hilmo/thl2019_1776_poisto_8793.csv.finreg_IDsp'
start_time = time.time()
hilmo2 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
v2 = pd.DataFrame()
```


```python
v2['FINREGISTRYID'] = hilmo2['TNRO']
v2['SOURCE'] = "INPAT"
```


```python
hilmo2['TUPVA'] = pd.to_datetime(hilmo2['TUPVA']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
v2 = v2.merge(dob, on='FINREGISTRYID', how='left') # add DOB
v2['DOB(YYYY-MM-DD)'] = pd.to_datetime(v2['DOB(YYYY-MM-DD)'])
v2['EVENT_AGE'] = (hilmo2['TUPVA'] - v2['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
v2.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
v2['PVM'] = hilmo2['TUPVA']
v2['EVENT_YRMNTH'] = hilmo2['TUPVA'].dt.strftime('%Y-%m')
```


```python
v2['CODE1'] = hilmo2['PDG']
v2['CODE2'] = hilmo2['SDG1']
v2['CODE3'] = np.nan
hilmo2['LPVM'] = pd.to_datetime(hilmo2['LPVM']) # DATE OF EVENT
v2['CODE4'] = (hilmo2['LPVM'] - hilmo2['TUPVA']).dt.days
```


```python
v2['ICDVER'] = 9
v2['CATEGORY'] = 2
```


```python
v2 = v2[v2['CODE1'].notna()].copy()
```


```python
v2['INDEX'] = np.arange(v2.shape[0])
v2['INDEX'] = v2['INDEX'].astype(str) + '_IN8793'
```


```python
######################
# 69-85
######################
path = '/data/processed_data/thl_hilmo/hilmo/thl2019_1776_poisto_6986.csv.finreg_IDsp'
start_time = time.time()
hilmo3 = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
v3 = pd.DataFrame()
```


```python
v3['FINREGISTRYID'] = hilmo3['TNRO']
v3['SOURCE'] = "INPAT"
```


```python
hilmo3['TULOPV'] = pd.to_datetime(hilmo3['TULOPV']) # DATE OF EVENT
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
v3 = v3.merge(dob, on='FINREGISTRYID', how='left') # add DOB
v3['DOB(YYYY-MM-DD)'] = pd.to_datetime(v3['DOB(YYYY-MM-DD)'])
v3['EVENT_AGE'] = (hilmo3['TULOPV'] - v3['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
v3.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
v3['PVM'] = hilmo3['TULOPV']
v3['EVENT_YRMNTH'] = hilmo3['TULOPV'].dt.strftime('%Y-%m')
```


```python
v3['CODE1'] = hilmo3['DG1']
v3['CODE2'] = hilmo3['DG2']
v3['CODE3'] = np.nan
hilmo3['LAHTOPV'] = pd.to_datetime(hilmo3['LAHTOPV']) # discharge day
v3['CODE4'] = (hilmo3['LAHTOPV'] - hilmo3['TULOPV']).dt.days
```


```python
v3['ICDVER'] = 8
v3['CATEGORY'] = 3
```


```python
v3 = v3[v3['CODE1'].notna()].copy()
```


```python
v3['INDEX'] = np.arange(v3.shape[0])
v3['INDEX'] = v3['INDEX'].astype(str) + '_IN6985'
```


```python
v123 = pd.concat([v1, v2, v3])
```


```python
print(v1.shape[0],v2.shape[0],v3.shape[0])
```


```python
# make Event age = death age for events recorded after death

path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
start_time = time.time()
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
run_time = time.time()-start_time
print(run_time)
```


```python
v123_death = v123.merge(death, on='FINREGISTRYID', how='left') 
```


```python
print("EVENT_AGE > DEATH_AGE:", v123_death[v123_death['EVENT_AGE_y']<v123_death['EVENT_AGE_x']].shape[0])
```


```python
v123_death['EVENT_AGE_QC'] = v123_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)
```


```python
#checks
print((v123_death['EVENT_AGE_x']>v123_death['EVENT_AGE_y']).value_counts(dropna=False))
print((v123_death['EVENT_AGE_QC']>v123_death['EVENT_AGE_y']).value_counts(dropna=False))
print(v123.shape,v123_death.shape)
```


```python
v123_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
v123_death = v123_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
v123_death = v123_death[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',v123_death[v123_death['EVENT_AGE']<0].shape[0])
v123_death.loc[v123_death['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
v123_death.fillna("NA", inplace=True)
```


```python
v123_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_inpat_ICD89.csv',index=False)
```
