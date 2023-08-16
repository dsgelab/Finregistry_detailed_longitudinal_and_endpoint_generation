

```python
import pandas as pd
import numpy as np
#ids = pd.read_csv('/data/processed_data/Finregistry_IDs_and_DOB.txt')
ids = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_DOB.txt', usecols = ['FINREGISTRYID'])
#  FINNGENID SAMPLE_DATE BL_AGE BL_YEAR FU_END_AGE SEX
```


```python
ids['SAMPLE_DATE'] = "NA"
ids['BL_AGE'] = "NA"
ids['BL_YEAR'] = "NA"
ids['FU_END_AGE'] = "NA"
```


```python
sex = pd.read_csv('/data/processed_data/dvv/Tulokset_1900-2010_tutkhenk_ja_sukulaiset.txt.finreg_IDsp', usecols = ['Relative_ID','Sex'])
```


```python
sex = sex.rename(columns = {'Relative_ID': 'FINREGISTRYID'})
```


```python
sex = sex[sex['FINREGISTRYID'].notna()]
duplicates = sex.duplicated(subset=['FINREGISTRYID'])
sex = sex[duplicates == False].copy()
```


```python
baseline = ids.merge(sex, on='FINREGISTRYID', how='left') 
```


```python
baseline[baseline['Sex'].isna()].shape[0] # 96028 sex of spouses who are not index individuals is missiong
```


```python
baseline['Sex'].value_counts(dropna=False)
```


```python
baseline['Sex'] = baseline['Sex'].apply(lambda x: 'male' if x == 1 else ('female' if x == 2 else "NA"))
```


```python
baseline['Sex'].value_counts(dropna=False)
```


```python
baseline = baseline.rename(columns = {'Sex': 'SEX'}) 
```


```python
baseline.to_csv('/data/processed_data/endpointer/supporting_files/2020/baseline.txt', sep='\t',index=False) 
```