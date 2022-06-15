
## OPERATIONS for data update period 2019-2021


```python
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/THL2021_2196_HILMO_TOIMP.csv.finreg_IDsp'
start_time = time.time()
oper = pd.read_csv(path,usecols=['HILMO_ID','N','TOIMP'])
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main2020_2021.csv'
start_time = time.time()
hilmo = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
```


```python
oper = oper.merge(hilmo, on='HILMO_ID', how='left')
del hilmo
gc.collect()
```


```python
oper.drop(columns=['HILMO_ID'], inplace=True)
```


```python
oper = oper.rename(columns = {'ERIK_AVO': 'SOURCE'})
oper['SOURCE'] = oper['SOURCE'].apply(lambda x: 'OPER_OUT' if x == 1 else 'OPER_IN')
```


```python
oper = oper.rename(columns = {'N': 'CATEGORY','TOIMP': 'CODE1'})
oper['CATEGORY'] = oper['CATEGORY'].apply(lambda x: 'NOM'+str(x))
oper['CODE2'] = np.nan
oper['CODE3'] = np.nan
oper = oper[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
print(oper[oper['PVM'].isna()])
print(oper[oper['CODE1'].isna()])
oper= oper[oper['PVM'].notna()] # drop NANs 1 row
oper= oper[oper['CODE1'].notna()] # drop NANs 20605 rows
```


```python
oper['ICDVER'] = oper['ICDVER'].astype(int)
```


```python
# remove eentires from before 2019
oper['year']=oper['EVENT_YRMNTH'].apply(lambda x: x[:4])
oper['year']=oper['year'].astype(int)
oper=oper[oper['year']>=2019]
del oper['year']
```


```python
oper.fillna("NA", inplace=True)
```


```python
oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_oper_ICD10_2019_2021.csv',index=False)
```

## OPERATIONS for a period up to 2019


```python
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo_toimenpide.csv.finreg_IDsp'
start_time = time.time()
oper = pd.read_csv(path,usecols=['HILMO_ID','N','TOIMP'])
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main.csv'
start_time = time.time()
hilmo = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
```


```python
oper = oper.merge(hilmo, on='HILMO_ID', how='left')
del hilmo
gc.collect()
```


```python
oper.drop(columns=['HILMO_ID'], inplace=True)
```


```python
oper = oper.rename(columns = {'ERIK_AVO': 'SOURCE'})
oper['SOURCE'] = oper['SOURCE'].apply(lambda x: 'OPER_OUT' if x == 1 else 'OPER_IN')
```


```python
oper = oper.rename(columns = {'N': 'CATEGORY','TOIMP': 'CODE1'})
```


```python
oper['CATEGORY'] = oper['CATEGORY'].apply(lambda x: 'NOM'+str(x))
```


```python
oper['CODE2'] = np.nan
oper['CODE3'] = np.nan
```


```python
oper = oper[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
print(oper[oper['PVM'].isna()])
oper= oper[oper['PVM'].notna()] # drop NANs 1 row
oper= oper[oper['CODE1'].notna()] # drop NANs 20605 rows
```


```python
oper['ICDVER'] = oper['ICDVER'].astype(int)
```


```python
oper.fillna("NA", inplace=True)
```


```python
oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_oper_ICD10.csv',index=False)
```


```python
oper = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_oper_ICD10.csv')
```


```python
# remove eentires from after 2018
oper['year']=oper['EVENT_YRMNTH'].apply(lambda x: x[:4])
oper['year']=oper['year'].astype(int)
oper=oper[oper['year']<2019]
del oper['year']
```


```python
oper.fillna("NA", inplace=True)
```


```python
oper.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_oper_ICD10_until_2018.csv',index=False)
```
