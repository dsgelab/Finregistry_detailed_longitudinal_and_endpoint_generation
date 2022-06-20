
## We did not recieve files for Mouth procedures for 2020-2021 period

## Moth procedures for a period up to 2020 (left 2020 as no new files)


```python
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
```


```python
mouth.drop(columns=['TOIMENPIDE_HAMMAS'], inplace=True)
```


```python
mouth = mouth.merge(main11_16, on='AVOHILMO_ID', how='left') 
```


```python
#checks
print(mouth.shape)
print((mouth['TNRO']==mouth['FINREGISTRYID']).value_counts())
```


```python
mouth = mouth[(mouth['TNRO']==mouth['FINREGISTRYID'])] # removed 25531 (0.035%) of values because there was no corresponding avohilmo_ID in main file
```


```python
del main11_16
gc.collect()
```


```python
mouth.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
```


```python
mouth = mouth.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
mouth['CATEGORY'] = mouth['CATEGORY'].apply(lambda x: 'MOP'+str(x))
```


```python
mouth['PVM'] = pd.to_datetime(mouth['PVM'])
mouth['EVENT_YRMNTH'] = mouth['PVM'].dt.strftime('%Y-%m')
```


```python
mouth['CODE2'] = np.nan
mouth['CODE3'] = np.nan
mouth['CODE4'] = np.nan
mouth['SOURCE'] = "PRIM_OUT"
mouth['ICDVER'] = 10
```


```python
mouth = mouth[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
mouth.fillna("NA", inplace=True)
```


```python
mouth.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_mouth_11_16.csv',index=False)
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
```


```python
mouth.drop(columns=['TOIMENPIDE_HAMMAS'], inplace=True)
```


```python
mouth = mouth.merge(main17_20, on='AVOHILMO_ID', how='left') 
```


```python
#checks
print(mouth.shape)
print((mouth['TNRO']==mouth['FINREGISTRYID']).value_counts())
```


```python
mouth = mouth[(mouth['TNRO']==mouth['FINREGISTRYID'])] # removed 66238 (0.19%) of values because there was no corresponding avohilmo_ID in main file
```


```python
del main17_20
gc.collect()
```


```python
mouth.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
```


```python
mouth = mouth.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
mouth['CATEGORY'] = mouth['CATEGORY'].apply(lambda x: 'MOP'+str(x))
```


```python
mouth['PVM'] = pd.to_datetime(mouth['PVM'])
mouth['EVENT_YRMNTH'] = mouth['PVM'].dt.strftime('%Y-%m')
```


```python
mouth['CODE2'] = np.nan
mouth['CODE3'] = np.nan
mouth['CODE4'] = np.nan
mouth['SOURCE'] = "PRIM_OUT"
mouth['ICDVER'] = 10
```


```python
mouth = mouth[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
mouth.fillna("NA", inplace=True)
```


```python
mouth.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_mouth_17_20.csv',index=False)
```