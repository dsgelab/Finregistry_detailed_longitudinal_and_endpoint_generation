
# Data update file for a period up to 2021 (inclusive)


```python
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/kela_reimbursement/81_522_2022_KORVAUSOIKEUDET.csv.finreg_IDsp'
start_time = time.time()
df = pd.read_csv(path) 
run_time = time.time()-start_time
print(run_time)
```


```python
#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
```


```python
Reimb = pd.DataFrame()
Reimb['FINREGISTRYID'] = df['HETU']
Reimb['SOURCE'] = "REIMB"
```


```python
Reimb['PVM'] = df['KORVAUSOIKEUS_ALPV']
Reimb['PVM'] = pd.to_datetime(Reimb['PVM'], format='%Y-%m-%d', errors='coerce') 
Reimb['EVENT_YRMNTH'] = Reimb['PVM'].dt.strftime('%Y-%m')
```


```python
# EVENT AGE
Reimb['PVM'] = pd.to_datetime(Reimb['PVM']) # diagnosis date
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
Reimb = Reimb.merge(dob, on='FINREGISTRYID', how='left') # add DOB
Reimb['DOB(YYYY-MM-DD)'] = pd.to_datetime(Reimb['DOB(YYYY-MM-DD)'])
Reimb['EVENT_AGE'] = (Reimb['PVM'] - Reimb['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
Reimb.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
Reimb['CODE1'] = df['KORVAUSOIKEUS_KOODI']
Reimb['CODE2'] = df['DIAGNOOSI_KOODI']
Reimb['CODE3'] = np.nan
Reimb['CODE4'] = np.nan
```


```python
# remove rows with EVENT_YRMNTH is NAN
print('EVENT_YRMNTH is NAN:', Reimb[Reimb['EVENT_YRMNTH'].isna()].shape[0])
Reimb = Reimb[Reimb['EVENT_YRMNTH'].notna()] # drop NANs 
```


```python
## 'ICDVER'
Reimb['ICDVER'] = pd.to_datetime(Reimb['EVENT_YRMNTH'], format='%Y-%m')#errors='coerce')
Reimb['ICDVER'] = Reimb['ICDVER'].dt.strftime('%Y')

def isNaN(string):
    return string != string

def icd(x):
    if isNaN(x):
        x = x
    elif int(x) < 1986:
        x = 8
    elif int(x) < 1996:
        x = 9
    elif int(x) >= 1996:
        x = 10
    else:
        print('ERROR',x)
    return x

Reimb['ICDVER'] = Reimb['ICDVER'].apply(icd)
Reimb['ICDVER'] = Reimb['ICDVER'].astype(int)
```


```python
Reimb['CATEGORY'] = np.nan
Reimb['INDEX'] = np.arange(Reimb.shape[0])
Reimb['INDEX'] = Reimb['INDEX'].astype(str) + '_Kr'
```


```python
# make Event age = death age for events recorded after death

path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
start_time = time.time()
death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
run_time = time.time()-start_time
print(run_time)
Reimb_death = Reimb.merge(death, on='FINREGISTRYID', how='left') 
print("EVENT_AGE > DEATH_AGE:", Reimb_death[Reimb_death['EVENT_AGE_y']<Reimb_death['EVENT_AGE_x']].shape[0])
Reimb_death['EVENT_AGE_QC'] = Reimb_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1) #(row['EVENT_AGE_y'].notna()) &
```


```python
#checks
print((Reimb_death['EVENT_AGE_x']>Reimb_death['EVENT_AGE_y']).value_counts(dropna=False))
print((Reimb_death['EVENT_AGE_QC']>Reimb_death['EVENT_AGE_y']).value_counts(dropna=False))
print(Reimb.shape,Reimb_death.shape)
```


```python
Reimb_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
Reimb_death = Reimb_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',Reimb_death[Reimb_death['EVENT_AGE']<0].shape[0])
Reimb_death.loc[Reimb_death['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
Reimb_death = Reimb_death[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
Reimb_death.fillna("NA", inplace=True)
```


```python
Reimb_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/Reimb_20220907.csv',index=False)
```

# Old file for a period up to 2019 (inclusive)


```python
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/kela_reimbursement/175_522_2020_LAAKEKORVAUSOIKEUDET.csv.finreg_IDsp'
start_time = time.time()
df = pd.read_csv(path) #, delim_whitespace=True) # error_bad_lines=False , sep = '/t', engine='python'  , header=None 
run_time = time.time()-start_time
print(run_time)
```


```python
#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']

```


```python
Reimb = pd.DataFrame()
```


```python
Reimb['FINREGISTRYID'] = df['HETU']
```


```python
Reimb['SOURCE'] = "REIMB"
```


```python
df['APVM'] = pd.to_datetime(df['APVM'], format='%Y-%m', errors='coerce') 
Reimb['EVENT_YRMNTH'] = df['APVM'].dt.strftime('%Y-%m')
```


```python
Reimb['PVM'] = df['ALPV']
```


```python
# cange NaNs in PVM to EVENT_YRMNTH" + 15d. 
print('PVM is NAN:', Reimb[Reimb['PVM'].isna()].shape[0])
def change(x):
    try:
        return x + '-15'
    except AttributeError:
        return np.nan
    except ValueError:
        return np.nan
    except TypeError:
        return np.nan

#Reimb['PVM2']=Reimb['PVM']

Reimb['EVENT_YRMNTH_15'] = Reimb['EVENT_YRMNTH'].apply(change)
Reimb.loc[Reimb['PVM'].isna(),'PVM'] = Reimb[Reimb['PVM'].isna()]['EVENT_YRMNTH_15']
Reimb.drop(columns='EVENT_YRMNTH_15', inplace=True)
```


```python
# EVENT AGE
Reimb['PVM'] = pd.to_datetime(Reimb['PVM']) # diagnosis date
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'
Reimb = Reimb.merge(dob, on='FINREGISTRYID', how='left') # add DOB
Reimb['DOB(YYYY-MM-DD)'] = pd.to_datetime(Reimb['DOB(YYYY-MM-DD)'])
Reimb['EVENT_AGE'] = (Reimb['PVM'] - Reimb['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
Reimb.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
```


```python
Reimb['CODE1'] = df['SK1']
Reimb['CODE2'] = df['DIAG']
Reimb['CODE3'] = np.nan
Reimb['CODE4'] = np.nan
```


```python
# remove rows with EVENT_YRMNTH is NAN
print('EVENT_YRMNTH is NAN:', Reimb[Reimb['EVENT_YRMNTH'].isna()].shape[0])
Reimb = Reimb[Reimb['EVENT_YRMNTH'].notna()] # drop NANs 
```


```python
## 'ICDVER'
Reimb['ICDVER'] = pd.to_datetime(Reimb['EVENT_YRMNTH'], format='%Y-%m')#errors='coerce')
Reimb['ICDVER'] = Reimb['ICDVER'].dt.strftime('%Y')

def isNaN(string):
    return string != string

def icd(x):
    if isNaN(x):
        x = x
    elif int(x) < 1986:
        x = 8
    elif int(x) < 1996:
        x = 9
    elif int(x) >= 1996:
        x = 10
    else:
        print('ERROR',x)
    return x

Reimb['ICDVER'] = Reimb['ICDVER'].apply(icd)
Reimb['ICDVER'] = Reimb['ICDVER'].astype(int)
```


```python
Reimb['CATEGORY'] = np.nan
```


```python
Reimb['INDEX'] = np.arange(Reimb.shape[0])
Reimb['INDEX'] = Reimb['INDEX'].astype(str) + '_Kr'
```


```python
Reimb = Reimb[(Reimb['CODE1'].notna()) | (Reimb['CODE2'].notna())] # exclude if both CODE1 and CODE2 are NaN
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
Reimb_death = Reimb.merge(death, on='FINREGISTRYID', how='left') 
```


```python
print("EVENT_AGE > DEATH_AGE:", Reimb_death[Reimb_death['EVENT_AGE_y']<Reimb_death['EVENT_AGE_x']].shape[0])
```


```python
Reimb_death['EVENT_AGE_QC'] = Reimb_death.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1) #(row['EVENT_AGE_y'].notna()) &
```


```python
#checks
print((Reimb_death['EVENT_AGE_x']>Reimb_death['EVENT_AGE_y']).value_counts(dropna=False))
print((Reimb_death['EVENT_AGE_QC']>Reimb_death['EVENT_AGE_y']).value_counts(dropna=False))
print(Reimb.shape,Reimb_death.shape)
```


```python
Reimb_death.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
Reimb_death = Reimb_death.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
```


```python
# change EVENT AGE < 0 to EVENT AGE == 0
print('EVENT AGE < 0:',Reimb_death[Reimb_death['EVENT_AGE']<0].shape[0])
Reimb_death.loc[Reimb_death['EVENT_AGE']<0,'EVENT_AGE'] = 0
```


```python
Reimb_death = Reimb_death[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
```


```python
Reimb_death.fillna("NA", inplace=True)
```


```python
Reimb_death.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/Reimb_20210622.csv',index=False)
```