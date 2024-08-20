# preferred way to run : python test.py &> log_test.txt

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from utils.func import *

from utils.config_test import *
DOB_map = DOB_map_preparation(filepath=DOB_map_file, sep=",", test=True)
paltu_map = pd.read_csv(PALTU_map_file, sep=",")  

import unittest

# -------------------
# Utility functions
def prepare_predicted_data(Data):
    Data = Data.drop(columns=['CODE5','CODE6','CODE7','CODE8','CODE9','INDEX']).sort_values(by=['FINREGISTRYID','EVENT_AGE','CATEGORY']).reset_index(drop=True)      
    return Data

def prepare_output_data(Data):
    Data = Data.drop(columns=['INDEX']).sort_values(by=['FINREGISTRYID','EVENT_AGE','CATEGORY']).reset_index(drop=True)
    return Data

def my_assert_frame_equal(PRED, TRUE, name='test', check_dtype=False):    
    assert_frame_equal(PRED, TRUE, check_dtype=check_dtype)         



# ------------------
# Test functions
class TestUtilityFunctions(unittest.TestCase):  

    def test_combination_codes_split(self):
        test_data = [
            ['K238',np.NaN,np.NaN],
            ['A000*B000',np.NaN,np.NaN],
            ['A000&B000',np.NaN,np.NaN],
            ['A000#B000',np.NaN,np.NaN],
            ['A000+B000',np.NaN,np.NaN],
            ['A000.B000',np.NaN,np.NaN],
            ['A000','B000',np.NaN]
            ]
        expected_output = [
            ['K238',np.NaN,np.NaN],
            ['A000','B000',np.NaN],
            ['A000','B000',np.NaN],
            ['A000',np.NaN,'B000'],
            ['B000','A000',np.NaN],
            ['A000.B000',np.NaN,np.NaN],
            ['A000','B000',np.NaN]
            ]        
        for row in range(len(test_data)):
            Data_IN = pd.DataFrame([test_data[row]], columns=['CODE1','CODE2','CODE3'])
            Data_OUT = pd.DataFrame([expected_output[row]], columns=['CODE1','CODE2','CODE3'])
            # print(Data_IN)
            assert_frame_equal(combination_codes_split(Data_IN),Data_OUT)

      
            
class TestRegistrySpecificFunctions(unittest.TestCase):  
    #NB: see config_test.py for all the file paths   
    
    def test_cancer(self):            
        global DOB_map
        global cancer_input, cancer_output, cancer_result
        
        CancerRegistry_processing(file_path=cancer_input, DOB_map=DOB_map, file_sep=",",test=True)     
        TRUE = pd.read_csv(cancer_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(cancer_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='cancer', check_dtype=False)  
           
    def test_death(self):            
        global DOB_map
        global death_input, death_output, death_result
        
        DeathRegistry_processing(file_path=death_input, DOB_map=DOB_map, file_sep=",",test=True)      
        TRUE = pd.read_csv(death_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(death_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='death', check_dtype=False)    
                
    def test_purchases(self):            
        global DOB_map
        global purch_input, purch_output, purch_result
        
        KelaPurchase_PRE20_processing(file_path=purch_input, DOB_map=DOB_map, file_sep=",",test=True)        
        TRUE = pd.read_csv(purch_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(purch_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='purchases', check_dtype=False)       
      
    def test_reimbursements(self):            
        global DOB_map
        global reimb_input, reimb_output, reimb_result
        
        KelaReimbursement_PRE20_processing(file_path=reimb_input, DOB_map=DOB_map, file_sep=",",test=True)      
        TRUE = pd.read_csv(reimb_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(reimb_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='reimbursements', check_dtype=False)     
    
    def test_hilmo_69_86(self):            
        global DOB_map
        global hilmo_69_86_input, hilmo_69_86_output, hilmo_69_86_result
        
        Hilmo_69_86_processing(file_path=hilmo_69_86_input, DOB_map=DOB_map, file_sep=",",test=True)      
        TRUE = pd.read_csv(hilmo_69_86_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_69_86_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='Hilmo 1969-1986', check_dtype=False)    

    def test_hilmo_87_93(self):            
        global DOB_map
        global paltu_map
        global hilmo_87_93_input, hilmo_87_93_output, hilmo_87_93_result
        
        Hilmo_87_93_processing(file_path=hilmo_87_93_input, DOB_map=DOB_map, file_sep=",", paltu_map=paltu_map, test=True)   
        TRUE = pd.read_csv(hilmo_87_93_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_87_93_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='Hilmo 1987-1993', check_dtype=False)    
          
    def test_hilmo_94_95(self):            
        global DOB_map
        global paltu_map
        global hilmo_94_95_input, hilmo_94_95_output, hilmo_94_95_result
        
        Hilmo_94_95_processing(file_path=hilmo_94_95_input, DOB_map=DOB_map, file_sep=",", paltu_map=paltu_map, test=True)      
        TRUE = pd.read_csv(hilmo_94_95_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_94_95_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='Hilmo 1994-1995', check_dtype=False)  
              
    def test_hilmo_96_18_diag(self): 
        global DOB_map
        global paltu_map
        global hilmo_96_18_base_input
        global hilmo_diag_input, hilmo_diag_output, hilmo_diag_result
        
        extra = Hilmo_extra_diagnosis_preparation(file_path=hilmo_diag_input, file_sep=",")     
        Hilmo_96_18_processing(file_path=hilmo_96_18_base_input, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra, extra_source='diag', file_sep=",", test=True)   
        TRUE = pd.read_csv(hilmo_diag_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_diag_result)
        PRED = prepare_predicted_data(PRED)
        PRED_NO_ULKSYY = PRED.loc[~(PRED.CATEGORY.str.contains('EX'))].reset_index(drop=True)
        my_assert_frame_equal(PRED_NO_ULKSYY, TRUE, name='Hilmo 1996-2018 + diag', check_dtype=False)          
        
    def test_hilmo_96_18_oper(self): 
        global DOB_map
        global paltu_map
        global hilmo_96_18_base_input
        global hilmo_oper_input, hilmo_oper_output, hilmo_oper_result
        
        extra = Hilmo_operations_preparation(file_path=hilmo_oper_input, file_sep=",")     
        Hilmo_96_18_processing(file_path=hilmo_96_18_base_input, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra, extra_source='oper', file_sep=",", test=True)   
        TRUE = pd.read_csv(hilmo_oper_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_oper_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='Hilmo 1996-2018 + oper', check_dtype=False)          
        
    def test_hilmo_96_18_heart(self): 
        global DOB_map
        global paltu_map
        global hilmo_96_18_base_input
        global hilmo_heart_input, hilmo_heart_output, hilmo_heart_result
        
        extra = Hilmo_heart_preparation(file_path=hilmo_heart_input, file_sep=",")     
        Hilmo_96_18_processing(file_path=hilmo_96_18_base_input, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra, extra_source='heart', file_sep=",", test=True)   
        TRUE = pd.read_csv(hilmo_heart_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_heart_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='Hilmo 1996-2018 + heart', check_dtype=False)          
        
    def test_hilmo_post18_diag(self): 
        global DOB_map
        global paltu_map
        global hilmo_post18_base_input
        global hilmo_diag_input, hilmo_post18_diag_output, hilmo_post18_diag_result
        
        extra = Hilmo_extra_diagnosis_preparation(file_path=hilmo_diag_input, file_sep=",")     
        Hilmo_POST18_processing(file_path=hilmo_post18_base_input, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra, extra_source='diag', file_sep=",", test=True)   
        TRUE = pd.read_csv(hilmo_post18_diag_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_post18_diag_result)
        PRED = prepare_predicted_data(PRED)
        PRED_NO_ULKSYY = PRED.loc[~(PRED.CATEGORY.str.contains('EX'))].reset_index(drop=True)
        my_assert_frame_equal(PRED_NO_ULKSYY, TRUE, name='Hilmo 2019-2021 + diag', check_dtype=False)          
        
    def test_hilmo_post18_oper(self): 
        global DOB_map
        global paltu_map
        global hilmo_post18_base_input
        global hilmo_oper_input, hilmo_post18_oper_output, hilmo_post18_oper_result
        
        extra = Hilmo_operations_preparation(file_path=hilmo_oper_input, file_sep=",")     
        Hilmo_POST18_processing(file_path=hilmo_post18_base_input, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra, extra_source='oper', file_sep=",", test=True)   
        TRUE = pd.read_csv(hilmo_post18_oper_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_post18_oper_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='Hilmo 2019-2021 + oper', check_dtype=False)          
        
    def test_hilmo_post18_heart(self): 
        global DOB_map
        global paltu_map
        global hilmo_post18_base_input
        global hilmo_heart_input, hilmo_post18_heart_output, hilmo_post18_heart_result
        
        extra = Hilmo_heart_preparation(file_path=hilmo_heart_input, file_sep=",")     
        Hilmo_POST18_processing(file_path=hilmo_post18_base_input, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra, extra_source='heart', file_sep=",", test=True)   
        TRUE = pd.read_csv(hilmo_post18_heart_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(hilmo_post18_heart_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='Hilmo 2019-2021 + heart', check_dtype=False)          
        
    def test_avohilmo_icd10(self):
        global DOB_map
        global paltu_map
        global avohilmo_base_input
        global avohilmo_icd10_input, avohilmo_icd10_output, avohilmo_icd10_result
        
        extra=AvoHilmo_codes_preparation(file_path=avohilmo_icd10_input, source='icd10', file_sep=",")
        AvoHilmo_processing(file_path=avohilmo_base_input, DOB_map=DOB_map, extra_to_merge=extra, source='icd10', year='', file_sep=",", test=True)      
        TRUE = pd.read_csv(avohilmo_icd10_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(avohilmo_icd10_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='AvoHilmo + icd10', check_dtype=False)  

    def test_avohilmo_icpc2(self):
        global DOB_map
        global paltu_map
        global avohilmo_base_input
        global avohilmo_icpc2_input, avohilmo_icpc2_output, avohilmo_icpc2_result
        
        extra=AvoHilmo_codes_preparation(file_path=avohilmo_icpc2_input, source='icpc2', file_sep=",")
        AvoHilmo_processing(file_path=avohilmo_base_input, DOB_map=DOB_map, extra_to_merge=extra, source='icpc2', year='', file_sep=",", test=True)      
        TRUE = pd.read_csv(avohilmo_icpc2_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(avohilmo_icpc2_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='AvoHilmo + icpc2', check_dtype=False)  

    def test_avohilmo_oral(self):
        global DOB_map
        global paltu_map
        global avohilmo_base_input
        global avohilmo_oral_input, avohilmo_oral_output, avohilmo_oral_result
        
        extra=AvoHilmo_codes_preparation(file_path=avohilmo_oral_input, source='oral', file_sep=",")
        AvoHilmo_processing(file_path=avohilmo_base_input, DOB_map=DOB_map, extra_to_merge=extra, source='oral', year='', file_sep=",", test=True)      
        TRUE = pd.read_csv(avohilmo_oral_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(avohilmo_oral_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='AvoHilmo + oral', check_dtype=False)  
        
    def test_avohilmo_oper(self):
        global DOB_map
        global paltu_map
        global avohilmo_base_input
        global avohilmo_oper_input, avohilmo_oper_output, avohilmo_oper_result
        
        extra=AvoHilmo_codes_preparation(file_path=avohilmo_oper_input, source='oper', file_sep=",")
        AvoHilmo_processing(file_path=avohilmo_base_input, DOB_map=DOB_map, extra_to_merge=extra, source='oper', year='', file_sep=",", test=True)      
        TRUE = pd.read_csv(avohilmo_oper_output,sep=',')
        TRUE = prepare_output_data(TRUE)
        PRED = pd.read_csv(avohilmo_oper_result)
        PRED = prepare_predicted_data(PRED)
        my_assert_frame_equal(PRED, TRUE, name='AvoHilmo + oper', check_dtype=False)  
        
            
if __name__ == '__main__':
    unittest.main()