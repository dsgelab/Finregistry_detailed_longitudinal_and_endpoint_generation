
# Variable descriptievs for data dictionary


```python
def variable_description3(df):
    import pandas as pd
    import gc
    import time


    col = list(df.columns)
    nrow = df.shape[0]
    def listToString(s): 

        # initialize an empty string
        str1 = "" 

        # traverse in the string  
        for ele in s:
            ele = str(ele)
            str1 += ele  
            str1 += ';'
        # return string  
        return str1[:-1] 

    rows = []

    for c in col:
        print(c)
        z = df[c]
        nns =  round( (z.isnull().sum(axis = 0)/nrow)*100, 2)
        unq = z.nunique()    
        z = z.astype(str)
        mx = z.max()
        mn = z.min()        
        freq = listToString(z.value_counts().index.tolist()[:5])
        rows.append([nrow, nns,unq, mn, mx,freq])
        del z
        gc.collect()
        
    result = pd.DataFrame(rows, columns=['NoOfRows','Missing values, %', 'NumberOfLevels','Min','Max','List of 5 most frequent levels'], index = col)
    #result.to_csv('',index=False) 
    del df
    gc.collect()

    return result
```


```python
#a1 = 22
#a2 = 42
import pandas as pd
import gc
sep = ';' # '\t' cancer / ';' DVV, THL
path = '/data/original_data/thl_hilmo/THL2021_2196_HILMO_2019_2021.csv.finreg_IDs'
df = pd.read_csv(path,sep = sep, encoding='latin-1')#,usecols=list(range(a1,a2)))
```


```python
import time
start_time = time.time()
result = variable_description3(df)
run_time = time.time()-start_time
print(run_time)
result
```
