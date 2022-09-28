#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
ids = pd.read_csv('/data/processed_data/dvv/Finregistry_IDs_and_DOB.txt', usecols = ['FINREGISTRYID'])


# In[2]:


ids.to_csv('/data/processed_data/endpointer/supporting_files/2020/custom_id_list.txt',index=False, header=False)

