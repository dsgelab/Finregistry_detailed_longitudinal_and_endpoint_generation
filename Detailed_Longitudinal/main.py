
##########################################################
# COPYRIGHT:  	THL/FIMM/Finregistry 2023  
# AUTHORS:     	Matteo Ferro, Essi Viippola
##########################################################


from datetime import datetime
import multiprocessing
import pandas as pd
import gc

# import all processing functions
from utils.func import *

# import all file paths
from utils.config import *


def preprocess_hilmo_69_86():
    START = datetime.now()
    Hilmo_69_86_processing(hilmo_1969_1986, DOB_map=DOB_map)
    END = datetime.now()
    print(f'hilmo_1969_1986 processing took {(END-START)} hour:min:sec')


def preprocess_hilmo_87_93():
    START = datetime.now()
    Hilmo_87_93_processing(hilmo_1987_1993, DOB_map=DOB_map, paltu_map=paltu_map)
    END = datetime.now()
    print(f'hilmo_1987_1993 processing took {(END-START)} hour:min:sec')


def preprocess_hilmo_94_95():
    START = datetime.now()
    Hilmo_94_95_processing(hilmo_1994_1995, DOB_map=DOB_map, paltu_map=paltu_map)
    extra_to_merge = Hilmo_heart_preparation(hilmo_heart_1994_1995)
    Hilmo_94_95_processing(hilmo_1994_1995, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra_to_merge)
    END = datetime.now()
    print(f'hilmo_1994_1995 processing took {(END-START)} hour:min:sec') 


def preprocess_hilmo_96_18():
    START = datetime.now()
    extra_to_merge = Hilmo_extra_diagnosis_preparation(hilmo_diag_1996_2018)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra_to_merge, extra_source='diag')
    extra_to_merge = Hilmo_heart_preparation(hilmo_heart_1996_2018)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra_to_merge, extra_source='heart')
    extra_to_merge = Hilmo_operations_preparation(hilmo_oper_1996_2018)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra_to_merge, extra_source='oper')
    END = datetime.now()
    print(f'hilmo_1996_2018 diagnosis processing took {(END-START)} hour:min:sec') 
    

def preprocess_hilmo_19_21():
    START = datetime.now()
    extra_to_merge = Hilmo_extra_diagnosis_preparation(hilmo_diag_2019_2021)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra_to_merge, estra_source='diag')  
    extra_to_merge = Hilmo_heart_preparation(hilmo_heart_2019_2021)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra_to_merge,extra_source='heart')   
    extra_to_merge = Hilmo_operations_preparation(hilmo_oper_2019_2021)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=extra_to_merge,extra_source='oper') 
    END = datetime.now()
    print(f'hilmo_2019_2021 processing took {(END-START)} hour:min:sec') 

    
def preprocess_avohilmo_icd10_year_11_16():
    START = datetime.now()  
    icd10_11_16 = AvoHilmo_codes_preparation(avohilmo_icd10_2011_2016, source='icd10')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_11_16, source='icd10', year='11_16')
    END = datetime.now()
    print(f'avohilmo + icd10 + year 2011-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icd10_year_17_19():
    START = datetime.now()  
    icd10_17_19 = AvoHilmo_codes_preparation(avohilmo_icd10_2017_2019, source='icd10')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_17_19, source='icd10', year='17_19')
    END = datetime.now()
    print(f'avohilmo + icd10 + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icd10_year_20_21():
    START = datetime.now()  
    icd10_20_21 = AvoHilmo_codes_preparation(avohilmo_icd10_2020_2021, source='icd10')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_20_21, source='icd10', year='20_21')
    END = datetime.now()
    print(f'avohilmo + icd10 + year 2020-2021 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icpc2_year_11_16():
    START = datetime.now()		
    icpc2_11_16 = AvoHilmo_codes_preparation(avohilmo_icpc2_2011_2016, source='icpc2')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_11_16, source='icpc2', year='11_16')
    END = datetime.now()
    print(f'avohilmo + icpc2 + year 2011-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icpc2_year_17_19():
    START = datetime.now()	
    icpc2_17_19 = AvoHilmo_codes_preparation(avohilmo_icpc2_2017_2019, source='icpc2')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_17_19, source='icpc2', year='17_19')	
    END = datetime.now()
    print(f'avohilmo + icpc2 + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icpc2_year_20_21():
    START = datetime.now()	
    icpc2_20_21 = AvoHilmo_codes_preparation(avohilmo_icpc2_2020_2021, source='icpc2')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_20_21, source='icpc2', year='20_21')	
    END = datetime.now()
    print(f'avohilmo + icpc2 + year 2020-2021 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oral_year_11_16():
    START = datetime.now()	
    oral_11_16 = AvoHilmo_codes_preparation(avohilmo_oral_2011_2016, source='oral')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]    
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_11_16, source='oral', year='11_16')
    END = datetime.now()
    print(f'avohilmo + oral + year 2011-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oral_year_17_19():
    START = datetime.now()    
    oral_17_19 = AvoHilmo_codes_preparation(avohilmo_oral_2017_2019, source='oral')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_17_19, source='oral', year='17_19')
    END = datetime.now()
    print(f'avohilmo + oral + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oral_year_20_21():
    START = datetime.now()    
    oral_20_21 = AvoHilmo_codes_preparation(avohilmo_oral_2020_2021, source='oral')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_20_21, source='oral', year='20_21')
    END = datetime.now()
    print(f'avohilmo + oral + year 2020-2021 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oper_year_11_16():
    START = datetime.now()	
    oper_11_16 = AvoHilmo_codes_preparation(avohilmo_oper_2011_2016, source='oper')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_11_16, source='oper', year='11_16')	
    END = datetime.now()
    print(f'avohilmo + oper + year 201-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oper_year_17_19():
    START = datetime.now()	    
    oper_17_19 = AvoHilmo_codes_preparation(avohilmo_oper_2017_2019, source='oper')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_17_19, source='oper', year='17_19')	
    END = datetime.now()
    print(f'avohilmo + oper + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oper_year_20_21():
    START = datetime.now()	
    oper_20_21 = AvoHilmo_codes_preparation(avohilmo_oper_2020_2021, source='oper')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_20_21, source='oper', year='20_21')	
    END = datetime.now()
    print(f'avohilmo + oper + year 2020-2021processing took {(END-START)} hour:min:sec')


def preprocess_death():
    START = datetime.now()
    DeathRegistry_processing(death_pre2020,DOB_map=DOB_map)
    DeathRegistry_processing(death_2020_2021,DOB_map=DOB_map)
    END = datetime.now()
    print(f'death registry processing took {(END-START)} hour:min:sec')


def preprocess_cancer():
    START = datetime.now()
    CancerRegistry_processing(cancer,DOB_map=DOB_map)
    END = datetime.now()
    print(f'cancer registry processing took {(END-START)} hour:min:sec')


def preprocess_kela_reimbursement():
    START = datetime.now()
    KelaReimbursement_PRE20_processing(kela_reimbursement_pre2020, DOB_map=DOB_map)
    KelaReimbursement_20_21_processing(kela_reimbursement_2020_2021, DOB_map=DOB_map)
    END = datetime.now()
    print(f'kela reimbursement processing took {(END-START)} hour:min:sec')


def preprocess_kela_purchases():
    START = datetime.now()
    for purchase_file in kela_purchase_pre2020:
        KelaPurchase_PRE20_processing(purchase_file,DOB_map=DOB_map)
    for purchase_file in kela_purchase_2020_2021:
        KelaPurchase_20_21_processing(purchase_file,DOB_map=DOB_map)
    END = datetime.now()
    print(f'kela purchases processing took {(END-START)} hour:min:sec')


if __name__ == '__main__':

    # prepare info to be added later: 
    DOB_map = DOB_map_preparation(MINIMAL_PHENOTYPE_PATH, sep=',')
    paltu_map = pd.read_csv("PALTU_mapping.csv", sep=",")

    # define processes, total number of CPUs required: 21
    # NB: Avohilmo and Hilmo separated in unique sub-processes for maximum speed
    processing_func_list = [
        preprocess_hilmo_69_86, 
        preprocess_hilmo_87_93,
        preprocess_hilmo_94_95,
        preprocess_hilmo_96_18,
        preprocess_hilmo_19_21,
        preprocess_avohilmo_icd10_year_11_16,
        preprocess_avohilmo_icd10_year_17_19,
        preprocess_avohilmo_icd10_year_20_21,
        preprocess_avohilmo_icpc2_year_11_16,
        preprocess_avohilmo_icpc2_year_17_19,
        preprocess_avohilmo_icpc2_year_20_21,
        preprocess_avohilmo_oral_year_11_16,
        preprocess_avohilmo_oral_year_17_19,
        preprocess_avohilmo_oral_year_20_21,
        preprocess_avohilmo_oper_year_11_16,
        preprocess_avohilmo_oper_year_17_19,
        preprocess_avohilmo_oper_year_20_21,
        preprocess_death,
        preprocess_cancer,
        preprocess_kela_reimbursement,
        preprocess_kela_purchases
    ]

    N_PROCESSES = len(processing_func_list)
    with multiprocessing.Pool(processes=N_PROCESSES) as pool:
        for func in processing_func_list:
            pool.apply_async(func)
        # Wait for all processes to finish
        pool.close()
        pool.join()

    print("Detailed Longitudinal file has been created!") 