#!/usr/bin/env python
# coding: utf-8

# ## Dignoses for data update period 2020-2021

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/THL2021_2196_AVOHILMO_ICD10_DIAG.csv.finreg_IDsp'
start_time = time.time()
diag = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_20_21.csv'
start_time = time.time()
main20_21 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


# In[ ]:


diag = diag.merge(main20_21, on='AVOHILMO_ID', how='left')


# In[ ]:


#checks
print(diag.shape)
print((diag['TNRO']==diag['FINREGISTRYID']).value_counts())


# In[ ]:


diag = diag[(diag['TNRO']==diag['FINREGISTRYID'])] # removed 3 (0.00001%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main20_21
gc.collect()


# In[ ]:


diag.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)
diag = diag.rename(columns = {'JARJESTYS': 'CATEGORY','ICD10': 'CODE1'})
diag['CATEGORY'] = diag['CATEGORY'].apply(lambda x: 'ICD'+str(x))


# In[ ]:


diag['PVM'] = pd.to_datetime(diag['PVM'])
diag['EVENT_YRMNTH'] = diag['PVM'].dt.strftime('%Y-%m')


# In[ ]:


# CHECK/REMOVE ENTRIES BEFORE 2020


# In[ ]:


diag['CODE2'] = np.nan
diag['CODE3'] = np.nan
diag['CODE4'] = np.nan
diag['SOURCE'] = "PRIM_OUT"
diag['ICDVER'] = 10
diag = diag[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]
diag.fillna("NA", inplace=True)


# In[ ]:


#len(diag['EVENT_YRMNTH'].value_counts().index.tolist())


# In[ ]:


diag.shape


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_20_21.csv',index=False)


# ### Cleaning 1: removing dots from ICD codes

# In[ ]:



diag = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_20_21.csv')

a = diag['CODE1'].value_counts().index.tolist()
elements = []
for element in a:
    if "." in element:
        elements.append(element)
print('unique codes with dots: ',len(elements))

def removedot(x):
    if "." in x:
        x = x.replace('.','')
    return x

diag['CODE1'] = diag['CODE1'].apply(removedot)

b = diag['CODE1'].value_counts().index.tolist()
elements2 = []
for element in b:
    if "." in element:
        elements2.append(element)
print('unique codes with dots after removing dots: ',len(elements2))

diag.fillna("NA", inplace=True)
diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_20_21.csv',index=False)


# ### Cleaning 2: correct/split/remove codes with special characters at the beginning/end of code or separating two codes

# In[ ]:


diag = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_20_21.csv')


# In[ ]:


diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')] # 660518 rows contain speccial characters


# In[ ]:


diag = diag[diag['CODE1']!="-1"] # 53176 removed
diag = diag[diag['CODE1']!="-2"] # 7516 removed


# In[ ]:


diag['CODE1'] = diag['CODE1'].map(lambda x: x.lstrip('@#&$%+-/*').rstrip('@#&$%+-/*')) # 415124 corrected (special characters are removed if thay exist at the begining or at the end of a code)


# In[ ]:


diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')] # 660518 rows contain speccial characters


# In[ ]:


diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')]['CODE1'].value_counts() 


# In[ ]:


print(diag.shape)
diag_tmp = diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')].copy()
print(diag_tmp.shape)
diag = diag[~diag.CODE1.str.contains(r'[@#&$%+-/*]')]
print(diag.shape)


# In[ ]:


uniq_ = diag_tmp[diag_tmp.CODE1.str.contains(r'[@#&$%+-/*]')]['CODE1'].value_counts().index.tolist() # After exploring all the rest of the codes have a special character separating towo ICD10 codes in the middle (special characters contain: '#&+*')


# In[ ]:


# The codes where special character separates towo ICD10 codes in the missdle are split into two codes and the first is placed in CODE1, the second in CODE2
import re
start_time = time.time()

def myfunc1(code):

    if any(x in code for x in r'[@#&$%+-/*]'):
        CODE1 = re.split('[@#&$%+-/*]', code)[0]
        CODE2 = re.split('[@#&$%+-/*]', code)[1]
    else:
        CODE1 = code
        CODE2 = np.nan          

    return pd.Series([CODE1, CODE2])

diag_tmp[['CODE1', 'CODE2']] = diag_tmp.apply(lambda x: myfunc1(x.CODE1) ,axis=1)

run_time = time.time()-start_time
print(run_time)


# In[ ]:


diag = pd.concat([diag, diag_tmp])


# In[ ]:


print(diag.shape)
diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_20_21.csv',index=False)


# ## Dignoses for a period up to 2020

# In[ ]:


###################################
# for 2011-2016 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_icd10.csv.finreg_IDsp'
start_time = time.time()
diag = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_11_16.csv'
start_time = time.time()
main11_16 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: ICD0:25	Causes of the visit according to ICD10 classification; 0=main diagnosis, 1:25 side diagnoses


# In[ ]:


diag = diag.merge(main11_16, on='AVOHILMO_ID', how='left')


# In[ ]:


#checks
print(diag.shape)
print((diag['TNRO']==diag['FINREGISTRYID']).value_counts())


# In[ ]:


diag = diag[(diag['TNRO']==diag['FINREGISTRYID'])] # removed 37583 (0.06%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main11_16
gc.collect()


# In[ ]:


diag.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


diag = diag.rename(columns = {'JARJESTYS': 'CATEGORY','ICD10': 'CODE1'})


# In[ ]:


diag['CATEGORY'] = diag['CATEGORY'].apply(lambda x: 'ICD'+str(x))


# In[ ]:


diag['PVM'] = pd.to_datetime(diag['PVM'])
diag['EVENT_YRMNTH'] = diag['PVM'].dt.strftime('%Y-%m')


# In[ ]:


diag['CODE2'] = np.nan
diag['CODE3'] = np.nan
diag['CODE4'] = np.nan
diag['SOURCE'] = "PRIM_OUT"
diag['ICDVER'] = 10


# In[ ]:


diag = diag[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_11_16.csv',index=False)


# ### Cleaning 1: removing dots from ICD codes

# In[ ]:



diag = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_11_16.csv')

a = diag['CODE1'].value_counts().index.tolist()
elements = []
for element in a:
    if "." in element:
        elements.append(element)
print('unique codes with dots: ',len(elements))

def removedot(x):
    if "." in x:
        x = x.replace('.','')
    return x

diag['CODE1'] = diag['CODE1'].apply(removedot)

b = diag['CODE1'].value_counts().index.tolist()
elements2 = []
for element in b:
    if "." in element:
        elements2.append(element)
print('unique codes with dots after removing dots: ',len(elements2))

diag.fillna("NA", inplace=True)
diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_11_16.csv',index=False)


# ### Cleaning 2: correct/split/remove codes with special characters at the beginning/end of code or separating two codes

# In[ ]:


diag = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_11_16.csv')


# In[ ]:


diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')] # 462180 rows contain speccial characters


# In[ ]:


diag = diag[diag['CODE1']!="-1"] # 14503 removed
diag = diag[diag['CODE1']!="-2"] # 1263 removed


# In[ ]:


diag['CODE1'] = diag['CODE1'].map(lambda x: x.lstrip('@#&$%+-/*').rstrip('@#&$%+-/*')) # 373671 corrected (special characters are removed if thay exist at the begining or at the end of a code)


# In[ ]:


diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')]['CODE1'].value_counts() 


# In[ ]:


print(diag.shape)
diag_tmp = diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')].copy()
print(diag_tmp.shape)
diag = diag[~diag.CODE1.str.contains(r'[@#&$%+-/*]')]
print(diag.shape)


# In[ ]:


uniq_ = diag_tmp[diag_tmp.CODE1.str.contains(r'[@#&$%+-/*]')]['CODE1'].value_counts().index.tolist() # After exploring all the rest of the codes have a special character separating towo ICD10 codes in the middle (special characters contain: '#&+*')


# In[ ]:


# The codes where special character separates towo ICD10 codes in the missdle are split into two codes and the first is placed in CODE1, the second in CODE2
import re
start_time = time.time()

def myfunc1(code):

    if any(x in code for x in r'[@#&$%+-/*]'):
        CODE1 = re.split('[@#&$%+-/*]', code)[0]
        CODE2 = re.split('[@#&$%+-/*]', code)[1]
    else:
        CODE1 = code
        CODE2 = np.nan          

    return pd.Series([CODE1, CODE2])

diag_tmp[['CODE1', 'CODE2']] = diag_tmp.apply(lambda x: myfunc1(x.CODE1) ,axis=1)

run_time = time.time()-start_time
print(run_time)


# In[ ]:


diag = pd.concat([diag, diag_tmp])


# In[ ]:


diag.shape


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_11_16.csv',index=False)


# In[ ]:


###################################
# for 2017-2020 period
###################################

import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_avohilmo/thl2019_1776_avohilmo_17_20_icd10.csv.finreg_IDsp'
start_time = time.time()
diag = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/avo_main_17_20.csv'
start_time = time.time()
main17_20 = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
#CATEGORY: ICD0:25	Causes of the visit according to ICD10 classification; 0=main diagnosis, 1:25 side diagnoses


# In[ ]:


diag = diag.merge(main17_20, on='AVOHILMO_ID', how='left')


# In[ ]:


#checks
print(diag.shape)
print((diag['TNRO']==diag['FINREGISTRYID']).value_counts())


# In[ ]:


diag = diag[(diag['TNRO']==diag['FINREGISTRYID'])] # removed 89837 (0.17%) of values because there was no corresponding avohilmo_ID in main file


# In[ ]:


del main17_20
gc.collect()


# In[ ]:


diag.drop(columns=['AVOHILMO_ID','TNRO'], inplace=True)


# In[ ]:


diag = diag.rename(columns = {'JARJESTYS': 'CATEGORY','ICD10': 'CODE1'})


# In[ ]:


diag['CATEGORY'] = diag['CATEGORY'].apply(lambda x: 'ICD'+str(x))


# In[ ]:


diag['PVM'] = pd.to_datetime(diag['PVM'])
diag['EVENT_YRMNTH'] = diag['PVM'].dt.strftime('%Y-%m')


# In[ ]:


diag['CODE2'] = np.nan
diag['CODE3'] = np.nan
diag['CODE4'] = np.nan
diag['SOURCE'] = "PRIM_OUT"
diag['ICDVER'] = 10


# In[ ]:


diag = diag[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_17_20.csv',index=False)


# ### Cleaning 1: removing dots from ICD codes

# In[ ]:


#### REMOVING dots from CODE1 #######
diag = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_17_20.csv')

a = diag['CODE1'].value_counts().index.tolist()
elements = []
for element in a:
    if "." in element:
        elements.append(element)
print('unique codes with dots: ',len(elements))

def removedot(x):
    if "." in x:
        x = x.replace('.','')
    return x

#diag['CODE1'] = diag['CODE1'].apply(removedot)

b = diag['CODE1'].value_counts().index.tolist()
elements2 = []
for element in b:
    if "." in element:
        elements2.append(element)
print('unique codes with dots after removing dots: ',len(elements2))


diag.fillna("NA", inplace=True)
diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_17_20.csv',index=False)


# ### Cleaning 2: correct/split/remove codes with special characters at the beginning/end of code or separating two codes

# In[ ]:


diag = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_17_20.csv')


# In[ ]:


diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')] # 710713 rows contain speccial characters


# In[ ]:


diag = diag[diag['CODE1']!="-1"] # 8629 removed
diag = diag[diag['CODE1']!="-2"] # 79210 removed


# In[ ]:


diag['CODE1'] = diag['CODE1'].map(lambda x: x.lstrip('@#&$%+-/*').rstrip('@#&$%+-/*')) # 545373‬ corrected (special characters are removed if thay exist at the begining or at the end of a code)


# In[ ]:


diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')]['CODE1'].value_counts() 


# In[ ]:


print(diag.shape)
diag_tmp = diag[diag.CODE1.str.contains(r'[@#&$%+-/*]')].copy()
print(diag_tmp.shape)
diag = diag[~diag.CODE1.str.contains(r'[@#&$%+-/*]')]
print(diag.shape)


# In[ ]:


uniq_ = diag_tmp[diag_tmp.CODE1.str.contains(r'[@#&$%+-/*]')]['CODE1'].value_counts().index.tolist() # After exploring all the rest of the codes have a special character separating towo ICD10 codes in the middle (special characters contain: '#&+*')


# In[ ]:


# The codes where special character separates towo ICD10 codes in the missdle are split into two codes and the first is placed in CODE1, the second in CODE2
import re
start_time = time.time()

def myfunc1(code):

    if any(x in code for x in r'[@#&$%+-/*]'):
        CODE1 = re.split('[@#&$%+-/*]', code)[0]
        CODE2 = re.split('[@#&$%+-/*]', code)[1]
    else:
        CODE1 = code
        CODE2 = np.nan          

    return pd.Series([CODE1, CODE2])

diag_tmp[['CODE1', 'CODE2']] = diag_tmp.apply(lambda x: myfunc1(x.CODE1) ,axis=1)

run_time = time.time()-start_time
print(run_time)


# In[ ]:


diag = pd.concat([diag, diag_tmp])


# In[ ]:


diag.shape


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_17_20.csv',index=False)


# ### Remove entries from the year 2020 (which is available and used from data update)

# In[ ]:


diag = pd.read_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_17_20.csv')


# In[ ]:


diag['year']=diag['EVENT_YRMNTH'].apply(lambda x: x[:4])
diag['year']=diag['year'].astype(int)
print(diag.shape)


# In[ ]:


diag['year'].value_counts()


# In[ ]:


diag=diag[diag['year']<2020]
del diag['year']
print(diag.shape)


# In[ ]:


diag.fillna("NA", inplace=True)


# In[ ]:


diag.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/avo_diag_17_19.csv',index=False)