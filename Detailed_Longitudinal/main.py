
##########################################################
# COPYRIGHT:  	THL/FIMM/Finregistry 2023  
# AUTHORS:     	Matteo Ferro, Essi Viippola
##########################################################


from datetime import datetime
import multiprocessing
import pandas as pd
import gc

# import all processing functions
from func import *

# import all file paths
from config import *

# fetch info to add later: 
DOB_map = DOB_map_preparation('/data/processed_data/minimal_phenotype/minimal_phenotype_2023-05-02.csv', sep=',')
paltu_map = pd.read_csv("PALTU_mapping.csv", sep=",")


def preprocess_hilmo():

    START = datetime.now()
    Hilmo_69_86_processing(hilmo_1969_1986, DOB_map=DOB_map)
    END = datetime.now()
    print(f'hilmo_1969_1986 processing took {(END-START)} hour:min:sec')


    START = datetime.now()
    Hilmo_87_93_processing(hilmo_1987_1993, DOB_map=DOB_map, paltu_map=paltu_map)
    END = datetime.now()
    print(f'hilmo_1987_1993 processing took {(END-START)} hour:min:sec')

# ---

    START = datetime.now()
    heart_94_95 = Hilmo_heart_preparation(hilmo_heart_1994_1995)
    Hilmo_94_95_processing(hilmo_1994_1995, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_94_95)
    END = datetime.now()
    print(f'hilmo_1994_1995 + heart processing took {(END-START)} hour:min:sec')
    del heart_94_95
    gc.collect() 


    START = datetime.now()
    heart_96_18 = Hilmo_heart_preparation(hilmo_heart_1996_2018)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_96_18)
    END = datetime.now()
    print(f'hilmo_1996_2018 + heart processing took {(END-START)} hour:min:sec')	
    del heart_96_18
    gc.collect() 


    START = datetime.now()
    heart_19_21 = Hilmo_heart_preparation(hilmo_heart_2019_2021)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_19_21)
    END = datetime.now()
    print(f'hilmo_2019_2021 + heart processing took {(END-START)} hour:min:sec')
    del heart_19_21
    gc.collect() 

# ---

    START = datetime.now()
    oper_96_18 = Hilmo_operations_preparation(hilmo_oper_1996_2018)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_96_18)
    END = datetime.now()
    print(f'hilmo_1996_2018 + oper processing took {(END-START)} hour:min:sec')
    del oper_96_18
    gc.collect() 


    START = datetime.now()
    oper_19_21 = Hilmo_operations_preparation(hilmo_oper_2019_2021)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_19_21) 
    END = datetime.now()
    print(f'hilmo_2019_2021 + oper processing took {(END-START)} hour:min:sec')
    del oper_19_21
    gc.collect() 

# ---

    START = datetime.now()
    diag_96_18 = Hilmo_diagnosis_preparation(hilmo_diag_1996_2018)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=diag_96_18)
    END = datetime.now()
    print(f'hilmo_1996_2018 + diag processing took {(END-START)} hour:min:sec')
    del diag_96_18
    gc.collect() 


    START = datetime.now()
    diag_19_21 = Hilmo_diagnosis_preparation(hilmo_diag_2019_2021)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=diag_19_21) 
    END = datetime.now()
    print(f'hilmo_2019_2021 + diag processing took {(END-START)} hour:min:sec')
    del diag_19_21
    gc.collect()  


def preprocess_avohilmo_icd10_year_11_16():
    START = datetime.now()  
    icd10_11_16 = AvoHilmo_codes_preparation(avohilmo_icd10_2011_2016, source='icd10')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_11_16, source='icd10', year='11_16')
    del icd10_11_16
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + icd10 + year 2011-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icd10_year_17_19():
    START = datetime.now()  
    icd10_17_19 = AvoHilmo_codes_preparation(avohilmo_icd10_2017_2019, source='icd10')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_17_19, source='icd10', year='17_19')
    del icd10_17_19
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + icd10 + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icd10_year_20_21():
    START = datetime.now()  
    icd10_20_21 = AvoHilmo_codes_preparation(avohilmo_icd10_2020_2021, source='icd10')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_20_21, source='icd10', year='20_21')
    del icd10_20_21
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + icd10 + year 2020-2021 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icpc2_year_11_16():
    START = datetime.now()		
    icpc2_11_16 = AvoHilmo_codes_preparation(avohilmo_icpc2_2011_2016, source='icpc2')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_11_16, source='icpc2', year='11_16')
    del icpc2_11_16
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + icpc2 + year 2011-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icpc2_year_17_19():
    START = datetime.now()	
    icpc2_17_19 = AvoHilmo_codes_preparation(avohilmo_icpc2_2017_2019, source='icpc2')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_17_19, source='icpc2', year='17_19')	
    del icpc2_17_19
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + icpc2 + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_icpc2_year_20_21():
    START = datetime.now()	
    icpc2_20_21 = AvoHilmo_codes_preparation(avohilmo_icpc2_2020_2021, source='icpc2')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_20_21, source='icpc2', year='20_21')	
    del icpc2_20_21
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + icpc2 + year 2020-2021 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oral_year_11_16():
    START = datetime.now()	
    oral_11_16 = AvoHilmo_codes_preparation(avohilmo_oral_2011_2016, source='oral')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]    
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_11_16, source='oral', year='11_16')
    del oral_11_16
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + oral + year 2011-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oral_year_17_19():
    START = datetime.now()    
    oral_17_19 = AvoHilmo_codes_preparation(avohilmo_oral_2017_2019, source='oral')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_17_19, source='oral', year='17_19')
    del oral_17_19
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + oral + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oral_year_20_21():
    START = datetime.now()    
    oral_20_21 = AvoHilmo_codes_preparation(avohilmo_oral_2020_2021, source='oral')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_20_21, source='oral', year='20_21')
    del oral_20_21
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + oral + year 2020-2021 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oper_year_11_16():
    START = datetime.now()	
    oper_11_16 = AvoHilmo_codes_preparation(avohilmo_oper_2011_2016, source='oper')
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_11_16, source='oper', year='11_16')	
    del oper_11_16
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + oper + year 201-2016 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oper_year_17_19():
    START = datetime.now()	    
    oper_17_19 = AvoHilmo_codes_preparation(avohilmo_oper_2017_2019, source='oper')
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_17_19, source='oper', year='17_19')	
    del oper_17_19
    gc.collect() 
    END = datetime.now()
    print(f'avohilmo + oper + year 2017-2019 processing took {(END-START)} hour:min:sec')


def preprocess_avohilmo_oper_year_20_21():
    START = datetime.now()	
    oper_20_21 = AvoHilmo_codes_preparation(avohilmo_oper_2020_2021, source='oper')
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_20_21, source='oper', year='20_21')
    del oper_20_21
    gc.collect() 	
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

    # define processes, total number of CPUs required: 17
    # NB: Avohilmo separated in 12 unique processes for maximum speed
    p_hilmo = multiprocessing.Process(target=preprocess_hilmo)
    p_avohilmo_icd10_11_16 = multiprocessing.Process(target=preprocess_avohilmo_icd10_year_11_16)
    p_avohilmo_icd10_17_19 = multiprocessing.Process(target=preprocess_avohilmo_icd10_year_17_19)
    p_avohilmo_icd10_20_21 = multiprocessing.Process(target=preprocess_avohilmo_icd10_year_20_21)
    p_avohilmo_icpc2_11_16 = multiprocessing.Process(target=preprocess_avohilmo_icpc2_year_11_16)
    p_avohilmo_icpc2_17_19 = multiprocessing.Process(target=preprocess_avohilmo_icpc2_year_17_19)
    p_avohilmo_icpc2_20_21 = multiprocessing.Process(target=preprocess_avohilmo_icpc2_year_20_21)
    p_avohilmo_oral_11_16 = multiprocessing.Process(target=preprocess_avohilmo_oral_year_11_16)
    p_avohilmo_oral_17_19 = multiprocessing.Process(target=preprocess_avohilmo_oral_year_17_19)
    p_avohilmo_oral_20_21 = multiprocessing.Process(target=preprocess_avohilmo_oral_year_20_21)
    p_avohilmo_oper_11_16 = multiprocessing.Process(target=preprocess_avohilmo_oper_year_11_16)
    p_avohilmo_oper_17_19 = multiprocessing.Process(target=preprocess_avohilmo_oper_year_17_19)
    p_avohilmo_oper_20_21 = multiprocessing.Process(target=preprocess_avohilmo_oper_year_20_21)
    p_death = multiprocessing.Process(target=preprocess_death)
    p_cancer = multiprocessing.Process(target=preprocess_cancer)
    p_kela_reimbursement = multiprocessing.Process(target=preprocess_kela_reimbursement)
    p_kela_purchases = multiprocessing.Process(target=preprocess_kela_purchases)

    # Start multiprocessing
    p_hilmo.start()
    p_avohilmo_icd10_11_16.start()
    p_avohilmo_icd10_17_19.start()
    p_avohilmo_icd10_20_21.start()
    p_avohilmo_icpc2_11_16.start()
    p_avohilmo_icpc2_17_19.start()
    p_avohilmo_icpc2_20_21.start()
    p_avohilmo_oral_11_16.start()
    p_avohilmo_oral_17_19.start()
    p_avohilmo_oral_20_21.start()
    p_avohilmo_oper_11_16.start()
    p_avohilmo_oper_17_19.start()
    p_avohilmo_oper_20_21.start()
    p_death.start()
    p_cancer.start()
    p_kela_reimbursement.start()
    p_kela_purchases.start()

    # Wait for all the processes to end
    p_hilmo.join()
    p_avohilmo_icd10_11_16.join()
    p_avohilmo_icd10_17_19.join()
    p_avohilmo_icd10_11_16.join()
    p_avohilmo_icpc2_11_16.join()
    p_avohilmo_icpc2_17_19.join()
    p_avohilmo_icpc2_20_21.join()
    p_avohilmo_oral_11_16.join()
    p_avohilmo_oral_17_19.join()
    p_avohilmo_oral_20_21.join()
    p_avohilmo_oper_11_16.join()
    p_avohilmo_oper_17_19.join()
    p_avohilmo_oper_20_21.join()
    p_death.join()
    p_cancer.join()
    p_kela_reimbursement.join()
    p_kela_purchases.join()

    print("Detailed Longitudinal file has been created!") 