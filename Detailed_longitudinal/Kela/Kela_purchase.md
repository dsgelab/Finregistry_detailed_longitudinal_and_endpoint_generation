
# New files up to 2021 (inclusive)


```python
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np

results = np.zeros([1,6])
dob = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'

for n in range(2020,2022):
    path = '/data/processed_data/kela_purchase/81_522_2022_LAAKEOSTOT_'+str(n)+'.csv.finreg_IDsp'
    start_time = time.time()
    df = pd.read_csv(path)
    run_time = time.time()-start_time;print(run_time)
    
    p = pd.DataFrame()
    p['FINREGISTRYID'] = df['HETU']
    p['SOURCE'] = "PURCH"
    
    df['OSTOPV'] = pd.to_datetime(df['OSTOPV']) # DATE OF EVENT   
    p = p.merge(dob, on='FINREGISTRYID', how='left') # add DOB
    p['DOB(YYYY-MM-DD)'] = pd.to_datetime(p['DOB(YYYY-MM-DD)'])
    p['EVENT_AGE'] = (df['OSTOPV'] - p['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
    p.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
    p['PVM'] = df['OSTOPV']
    PVMnan = p[p['PVM'].isna()].shape[0]
    p['EVENT_YRMNTH'] = df['OSTOPV'].dt.strftime('%Y-%m')
    p['CODE1'] = df['ATC']
    p['CODE2'] = df['SAIR']
    p['CODE3'] = df['VNRO']
    p['CODE4'] = df['PLKM']
    p['ICDVER'] = np.nan
    p['CATEGORY'] = np.nan
    
    b = p.shape[0]
    p = p[(p['CODE1'].notna()) | (p['CODE2'].notna())].copy() # exclude if both CODE1 and CODE2 are NaN
    excludes = str(b-p.shape[0])
    
    a = p.shape[0]
    p.drop_duplicates(inplace=True)
    duplicates= str(a-p.shape[0])
    
    
    p['INDEX'] = np.arange(p.shape[0])
    p['INDEX'] = p['INDEX'].astype(str) + '_' + str(n)
    
    # make Event age = death age for events recorded after death
    path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/COD_forchangingEVENTAGE.csv'
    death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
    p = p.merge(death, on='FINREGISTRYID', how='left')
    evafterD = p[p['EVENT_AGE_y']<p['EVENT_AGE_x']].shape[0]
    p['EVENT_AGE_QC'] = p.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)
    p.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
    p = p.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
    
    # change EVENT AGE < 0 to EVENT AGE == 0
    evbeforeB = p[p['EVENT_AGE']<0].shape[0]
    p.loc[p['EVENT_AGE']<0,'EVENT_AGE'] = 0
    print(n," excludes: ",excludes," duplicates: ",duplicates," PVM NA: ",PVMnan," After D: ",evafterD," Before B: ",evbeforeB)

    p = p[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
    p.fillna("NA", inplace=True)
    
    results = np.concatenate([results,np.concatenate([[[n]],[[excludes]],[[duplicates]],[[PVMnan]],[[evafterD]],[[evbeforeB]]], axis = 1)])
    w_path = '//data/processed_data/detailed_longitudinal/supporting_files/kela_purch_'+str(n)+'.csv'
    p.to_csv(w_path,index=False)    
    
    del df
    del p
    gc.collect()    
```

# Old files up to 2019 (inclusive)


```python
import pandas as pd
import gc
import time
import datetime as dt
import numpy as np

results = np.zeros([1,6])
dob = pd.read_csv('/data/processed_data/Finregistry_IDs_and_full_DOB.txt') #  'DOB(YYYY-MM-DD)'

for n in range(1995,2020):
    path = '/data/processed_data/kela/175_522_2020_LAAKEOSTOT_'+str(n)+'.csv.finreg_IDsp'
    start_time = time.time()
    df = pd.read_csv(path)
    run_time = time.time()-start_time;print(run_time)
    
    p = pd.DataFrame()
    p['FINREGISTRYID'] = df['HETU']
    p['SOURCE'] = "PURCH"
    
    df['OSTOPV'] = pd.to_datetime(df['OSTOPV']) # DATE OF EVENT   
    p = p.merge(dob, on='FINREGISTRYID', how='left') # add DOB
    p['DOB(YYYY-MM-DD)'] = pd.to_datetime(p['DOB(YYYY-MM-DD)'])
    p['EVENT_AGE'] = (df['OSTOPV'] - p['DOB(YYYY-MM-DD)']).dt.days/365.24 # Event age = event date - DOB
    p.drop(columns='DOB(YYYY-MM-DD)', inplace=True)
    p['PVM'] = df['OSTOPV']
    PVMnan = p[p['PVM'].isna()].shape[0]
    p['EVENT_YRMNTH'] = df['OSTOPV'].dt.strftime('%Y-%m')
    p['CODE1'] = df['ATC']
    p['CODE2'] = df['SAIR']
    p['CODE3'] = df['VNRO']
    p['CODE4'] = df['PLKM']
    p['ICDVER'] = np.nan
    p['CATEGORY'] = np.nan
    
    b = p.shape[0]
    p = p[(p['CODE1'].notna()) | (p['CODE2'].notna())].copy() # exclude if both CODE1 and CODE2 are NaN
    excludes = str(b-p.shape[0])
    
    a = p.shape[0]
    p.drop_duplicates(inplace=True)
    duplicates= str(a-p.shape[0])
    
    
    p['INDEX'] = np.arange(p.shape[0])
    p['INDEX'] = p['INDEX'].astype(str) + '_' + str(n)
    
    # make Event age = death age for events recorded after death
    path = '/data/processed_data/detailed_longitudinal/supporting_files/COD_forchangingEVENTAGE.csv'
    death = pd.read_csv(path,usecols=['FINREGISTRYID','EVENT_AGE'])
    p = p.merge(death, on='FINREGISTRYID', how='left')
    evafterD = p[p['EVENT_AGE_y']<p['EVENT_AGE_x']].shape[0]
    p['EVENT_AGE_QC'] = p.apply(lambda row: row['EVENT_AGE_y'] if (row['EVENT_AGE_y']<row['EVENT_AGE_x']) else row['EVENT_AGE_x'], axis=1)
    p.drop(columns=['EVENT_AGE_x','EVENT_AGE_y'], inplace=True)
    p = p.rename(columns = {'EVENT_AGE_QC': 'EVENT_AGE'})
    
    # change EVENT AGE < 0 to EVENT AGE == 0
    evbeforeB = p[p['EVENT_AGE']<0].shape[0]
    p.loc[p['EVENT_AGE']<0,'EVENT_AGE'] = 0
    print(n," excludes: ",excludes," duplicates: ",duplicates," PVM NA: ",PVMnan," After D: ",evafterD," Before B: ",evbeforeB)

    p = p[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
    p.fillna("NA", inplace=True)
    
    results = np.concatenate([results,np.concatenate([[[n]],[[excludes]],[[duplicates]],[[PVMnan]],[[evafterD]],[[evbeforeB]]], axis = 1)])
    w_path = '/data/processed_data/detailed_longitudinal/kela_purch_'+str(n)+'.csv'
    p.to_csv(w_path,index=False)    
    
    del df
    del p
    gc.collect()    
```