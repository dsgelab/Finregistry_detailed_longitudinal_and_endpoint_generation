#!/usr/bin/env python
# coding: utf-8

# ## HEART PROCEDURES for data update period 2019-2021

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/THL2021_2196_HILMO_SYP.csv.finreg_IDsp'
start_time = time.time()
heart = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main2020_2021.csv'
start_time = time.time()
hilmo = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
2.037022113800049


# In[ ]:


heart = heart.merge(hilmo, on='HILMO_ID', how='left') 
del hilmo
gc.collect()


# In[ ]:


############################################################
# HPN1:4 - Procedure for demanding heart patient, NEW coding
############################################################


# In[ ]:


o12 = pd.DataFrame()
o12 = heart[['FINREGISTRYID','ERIK_AVO','PVM','EVENT_YRMNTH','ICDVER','INDEX','CODE4','EVENT_AGE']]
o12 = o12.rename(columns = {'ERIK_AVO': 'SOURCE'})
o12['SOURCE'] = o12['SOURCE'].apply(lambda x: 'OPER_OUT' if x == 1 else 'OPER_IN')


# In[ ]:


o12['CODE1'] = heart['TMPC1']
o12['CODE2'] = np.nan
o12['CODE3'] = np.nan
o12['CATEGORY'] = "HPN1"


# In[ ]:


o12 = o12[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


o13 = o12.copy()
o13['CODE1'] = heart['TMPC2']
o13['CATEGORY'] = "HPN2"


# In[ ]:


o14 = o12.copy()
o14['CODE1'] = heart['TMPC3']
o14['CATEGORY'] = "HPN3"


# In[ ]:


o15 = o12.copy()
o15['CODE1'] = heart['TMPC4']
o15['CATEGORY'] = "HPN4"


# In[ ]:


h_all = pd.concat([o12, o13, o14, o15])
print(h_all.shape[0] == o12.shape[0]*4)


# In[ ]:


h_all = h_all[h_all['CODE1'].notna()].copy()


# In[ ]:


h_all[h_all['PVM'].isna()]


# In[ ]:


# remove eentires from before 2019
h_all['year']=h_all['EVENT_YRMNTH'].apply(lambda x: x[:4])
h_all['year']=h_all['year'].astype(int)
h_all=h_all[h_all['year']>=2019]
del h_all['year']


# In[ ]:


h_all=h_all[h_all['year']>=2019]
del h_all['year']


# In[ ]:


h_all.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_heart_ICD10_2019_2021.csv',index=False)


# ## HEART PROCEDURES for a period up to 2019

# In[ ]:


import pandas as pd
import gc
import time
import datetime as dt
import numpy as np


path = '/data/processed_data/thl_hilmo/thl2019_1776_hilmo_syp.csv.finreg_IDsp'
start_time = time.time()
heart = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)


path = '/data/processed_data/detailed_longitudinal/supporting_files/additional_files/hilmo_main.csv'
start_time = time.time()
hilmo = pd.read_csv(path)
run_time = time.time()-start_time
print(run_time)

#header = ['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX',
# 'offset', 'APPROX_EVENT_DAY']
2.037022113800049


# In[ ]:


heart = heart.merge(hilmo, on='HILMO_ID', how='left') 
del hilmo
gc.collect()


# In[ ]:


############################################################
# HPO1:3 - Procedure for demanding heart patient, OLD coding
############################################################
o12 = pd.DataFrame()


# In[ ]:


o12 = heart[['FINREGISTRYID','ERIK_AVO','PVM','EVENT_YRMNTH','ICDVER','INDEX','CODE4','EVENT_AGE']]


# In[ ]:


o12 = o12.rename(columns = {'ERIK_AVO': 'SOURCE'})
o12['SOURCE'] = o12['SOURCE'].apply(lambda x: 'OPER_OUT' if x == 1 else 'OPER_IN')


# In[ ]:


o12['CODE1'] = heart['TMPTYP1']
o12['CODE2'] = np.nan
o12['CODE3'] = np.nan
o12['CATEGORY'] = "HPO1"


# In[ ]:


o12 = o12[['FINREGISTRYID', 'SOURCE', 'EVENT_AGE', 'PVM', 'EVENT_YRMNTH', 'CODE1', 'CODE2', 'CODE3', 'CODE4', 'ICDVER', 'CATEGORY', 'INDEX']]


# In[ ]:


o13 = o12.copy()
o13['CODE1'] = heart['TMPTYP2']
o13['CATEGORY'] = "HPO2"


# In[ ]:


o14 = o12.copy()
o14['CODE1'] = heart['TMPTYP3']
o14['CATEGORY'] = "HPO3"


# In[ ]:


############################################################
# HPN1:11 - Procedure for demanding heart patient, NEW coding
############################################################


# In[ ]:


o15 = o12.copy()
o15['CODE1'] = heart['TMPC1']
o15['CATEGORY'] = "HPN1"


# In[ ]:


o16 = o12.copy()
o16['CODE1'] = heart['TMPC2']
o16['CATEGORY'] = "HPN2"


# In[ ]:


o17 = o12.copy()
o17['CODE1'] = heart['TMPC3']
o17['CATEGORY'] = "HPN3"


# In[ ]:


o18 = o12.copy()
o18['CODE1'] = heart['TMPC4']
o18['CATEGORY'] = "HPN4"


# In[ ]:


o19 = o12.copy()
o19['CODE1'] = heart['TMPC5']
o19['CATEGORY'] = "HPN5"


# In[ ]:


o20 = o12.copy()
o20['CODE1'] = heart['TMPC6']
o20['CATEGORY'] = "HPN6"


# In[ ]:


o21 = o12.copy()
o21['CODE1'] = heart['TMPC7']
o21['CATEGORY'] = "HPN7"


# In[ ]:


o22 = o12.copy()
o22['CODE1'] = heart['TMPC8']
o22['CATEGORY'] = "HPN8"


# In[ ]:


o23 = o12.copy()
o23['CODE1'] = heart['TMPC9']
o23['CATEGORY'] = "HPN9"


# In[ ]:


o24 = o12.copy()
o24['CODE1'] = heart['TMPC10']
o24['CATEGORY'] = "HPN10"


# In[ ]:


o25 = o12.copy()
o25['CODE1'] = heart['TMPC11']
o25['CATEGORY'] = "HPN11"


# In[ ]:


h_all = pd.concat([o12, o13, o14, o15, o16, o17, o18, o19, o20, o21, o22, o23, o24, o25])
print(h_all.shape[0] == o12.shape[0]*14)


# In[ ]:


h_all = h_all[h_all['CODE1'].notna()].copy()


# In[ ]:


h_all[h_all['PVM'].isna()]


# In[ ]:


h_all.fillna("NA", inplace=True)


# In[ ]:


h_all.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_heart_ICD10.csv',index=False)


# In[ ]:


# remove eentires from after 2018
h_all['year']=h_all['EVENT_YRMNTH'].apply(lambda x: x[:4])
h_all['year']=h_all['year'].astype(int)
h_all=h_all[h_all['year']<2019]
del h_all['year']


# In[ ]:


h_all.to_csv('/data/processed_data/detailed_longitudinal/supporting_files/hilmo_heart_ICD10_until_2018.csv',index=False)
