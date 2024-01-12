
# ToRun:
# 1. activate shared conda environment
# 2. python test.py -v

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from func import *
import unittest


class TestBaseFunctions(unittest.TestCase):  

    def test_combination_codes_split(self):
        test_data = [
            ['K238',np.NaN,np.NaN],
            ['A000*B000',np.NaN,np.NaN],
            ['A000&B000',np.NaN,np.NaN],
            ['A000#B000',np.NaN,np.NaN],
            ['A000+B000',np.NaN,np.NaN]
            ]
        expected_output = [
            ['K238',np.NaN,np.NaN],
            ['A000','B000',np.NaN],
            ['A000','B000',np.NaN],
            ['A000',np.NaN,'B000'],
            ['B000','A000',np.NaN]
            ]        
        for row in range(len(test_data)):
            Data_IN = pd.DataFrame([test_data[row]], columns=['CODE1','CODE2','CODE3'])
            Data_OUT = pd.DataFrame([expected_output[row]], columns=['CODE1','CODE2','CODE3'])
            # print(Data_IN)
            assert_frame_equal(combination_codes_split(Data_IN),Data_OUT)


    def test_define_INPAT(self):
        test_data = [
            ['OUTPAT','1980-04-01',1,'R10'],
            ['OUTPAT','1980-04-01',2,'R10'],
            ['OUTPAT','1980-04-01',1,'R20'],
            #---          
            ['OUTPAT','2000-04-01',np.NaN,'R10'],
            ['OUTPAT','2000-04-01','','R10'],
            ['OUTPAT','2000-04-01',1,'R10'],
            ['OUTPAT','2000-04-01',2,'R10'],
            #---
            ['OUTPAT','2020-04-01',1,'R80'],
            ['OUTPAT','2020-04-01',1,'R10'],
            ['OUTPAT','2020-04-01',2,'R10'],
            ['OUTPAT','2020-04-01',np.NaN,'R10'],
            ['OUTPAT','2020-04-01',1,''],
            ['OUTPAT','2020-04-01',1,np.NaN],
            ['OUTPAT','2020-04-01',2,np.NaN]
            ]
        expected_output = [
            ['INPAT','1980-04-01',1,'R10'],
            ['INPAT','1980-04-01',2,'R10'],
            ['INPAT','1980-04-01',1,'R20'],
            #---
            ['OUTPAT','2000-04-01',np.NaN,'R10'],
            ['OUTPAT','2000-04-01','','R10'],
            ['INPAT','2000-04-01',1,'R10'],
            ['OUTPAT','2000-04-01',2,'R10'],
            #---
            ['INPAT','2020-04-01',1,'R80'],
            ['INPAT','2020-04-01',1,'R10'],
            ['OUTPAT','2020-04-01',2,'R10'],
            ['OUTPAT','2020-04-01',np.NaN,'R10'],
            ['INPAT','2020-04-01',1,''],
            ['INPAT','2020-04-01',1,np.NaN],
            ['OUTPAT','2020-04-01',2,np.NaN]
            ]   
        for row in range(len(test_data)):
            Data_IN = pd.DataFrame([test_data[row]], columns=['SOURCE','EVENT_DAY','PALA','YHTEYSTAPA'])
            Data_IN['EVENT_DAY'] = pd.to_datetime( Data_IN.EVENT_DAY,  format="%Y-%m-%d",errors="coerce")
            Data_OUT = pd.DataFrame([expected_output[row]], columns=['SOURCE','EVENT_DAY','PALA','YHTEYSTAPA'])
            # print(Data_IN)
            assert_series_equal(Define_INPAT(Data_IN).SOURCE,Data_OUT.SOURCE)


    def test_define_OPERIN(self):
        test_data = [
            ['INPAT','NOM1'],
            ['INPAT','HPN2'],
            ['INPAT','HPO1'],
            ['INPAT','0'],
            ]
        expected_output = [
            ['OPER_IN','NOM1'],
            ['OPER_IN','HPN2'],
            ['OPER_IN','HPO1'],
            ['INPAT','0']
            ]   
        for row in range(len(test_data)):
            Data_IN = pd.DataFrame([test_data[row]], columns=['SOURCE','CATEGORY'])
            Data_OUT = pd.DataFrame([expected_output[row]], columns=['SOURCE','CATEGORY'])
            # print(Data_IN)
            assert_series_equal(Define_OPERIN(Data_IN).SOURCE,Data_OUT.SOURCE)


    def test_define_OPEROUT(self):
        test_data = [
            ['OUTPAT','NOM1'],
            ['OUTPAT','HPN2'],
            ['OUTPAT','HPO1'],
            ['OUTPAT','0'],
            ]
        expected_output = [
            ['OPER_OUT','NOM1'],
            ['OPER_OUT','HPN2'],
            ['OPER_OUT','HPO1'],
            ['OUTPAT','0']
            ]   
        for row in range(len(test_data)):
            Data_IN = pd.DataFrame([test_data[row]], columns=['SOURCE','CATEGORY'])
            Data_OUT = pd.DataFrame([expected_output[row]], columns=['SOURCE','CATEGORY'])
            # print(Data_IN)
            assert_series_equal(Define_OPEROUT(Data_IN).SOURCE,Data_OUT.SOURCE)


    def test_fix_missing_value(self):
        test_data = [
            ['-1','3','3','3','3','3','3',-1,'3'],
            [-1,'3','3','3','3','3','3','3','3'],
            ['3','3','3','3','3','3','3','3','3'],
            ['3','3','3','3','3','-1','3','3','3'],
            ]
        expected_output = [
            [np.NaN,'3','3','3','3','3','3',np.NaN,'3'],
            [np.NaN,'3','3','3','3','3','3','3','3'],
            ['3','3','3','3','3','3','3','3','3'],
            ['3','3','3','3','3',np.NaN,'3','3','3'],
            ]  
        for row in range(len(test_data)):
            Data_IN = pd.DataFrame([test_data[row]], columns=['CODE1','CODE2','CODE3','CODE4','CODE5','CODE6','CODE7','CODE8','CODE9'])
            Data_OUT = pd.DataFrame([expected_output[row]], columns=['CODE1','CODE2','CODE3','CODE4','CODE5','CODE6','CODE7','CODE8','CODE9'])
            # print(Data_IN)
            assert_frame_equal(fix_missing_value(Data_IN),Data_OUT, check_dtype=False)

class TestRegistryScripts(unittest.TestCase):
    
    pass

if __name__ == '__main__':
    unittest.main()
