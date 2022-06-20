
## We did not recieve files for operations for 2020-2021 period

## Operations for a period up to 2020 (left 2020 as no new files)


```python
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
```


```python
oper = oper.merge(main11_16, on='AVOHILMO_ID', how='left') 
```


```python
#checks
print(oper.shape)
print((oper['TNRO']==oper['FINREGISTRYID']).value_counts())
```


```python
oper = oper[(oper['TNRO']==oper['FINREGISTRYID'])] # removed 48219 (0.029%) of values because there was no corresponding avohilmo_ID in main file
```


```python
del main11_16
gc.collect()
```


```python
oper.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
```


```python
oper = oper.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
oper['CATEGORY'] = oper['CATEGORY'].apply(lambda x: 'OP'+str(x))
```


```python
oper['PVM'] = pd.to_datetime(oper['PVM'])
oper['EVENT_YRMNTH'] = oper['PVM'].dt.strftime('%Y-%m')
```


```python
oper['CODE2'] = np.nan
oper['CODE3'] = np.nan
oper['CODE4'] = np.nan
oper['SOURCE'] = "PRIM_OUT"
oper['ICDVER'] = 10
```


```python
oper = oper[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
oper.fillna("NA", inplace=True)
```


```python
oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_oper_11_16.csv',index=False)
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
```


```python
oper = oper.merge(main17_20, on='AVOHILMO_ID', how='left') 
```


```python
#checks
print(oper.shape)
print((oper['TNRO']==oper['FINREGISTRYID']).value_counts())
```


```python
oper = oper[(oper['TNRO']==oper['FINREGISTRYID'])] # removed 97052 (0.05%) of values because there was no corresponding avohilmo_ID in main file
```


```python
del main17_20
gc.collect()
```


```python
oper.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
```


```python
oper = oper.rename(columns = {'JARJESTYS': 'CATEGORY','TOIMENPIDE': 'CODE1'})
oper['CATEGORY'] = oper['CATEGORY'].apply(lambda x: 'OP'+str(x))
```


```python
oper['PVM'] = pd.to_datetime(oper['PVM'])
oper['EVENT_YRMNTH'] = oper['PVM'].dt.strftime('%Y-%m')
```


```python
oper['CODE2'] = np.nan
oper['CODE3'] = np.nan
oper['CODE4'] = np.nan
oper['SOURCE'] = "PRIM_OUT"
oper['ICDVER'] = 10
```


```python
oper = oper[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
oper.fillna("NA", inplace=True)
```


```python
oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_oper_17_20.csv',index=False)
```