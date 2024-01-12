
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

# TODO: 
# - each of the functions here needs to write to a different output file
# - separate hilmo and avohilmo further into more processes 

def preprocess_hilmo():

    START = datetime.now()
    Hilmo_69_86_processing(hilmo_1969_1986, DOB_map=DOB_map, test=True)
    END = datetime.now()
    print(f'hilmo_1969_1986 processing took {(END-START)} hour:min:sec')


    START = datetime.now()
    Hilmo_87_93_processing(hilmo_1987_1993, DOB_map=DOB_map, paltu_map=paltu_map, test=True)
    END = datetime.now()
    print(f'hilmo_1987_1993 processing took {(END-START)} hour:min:sec')

# ---

    START = datetime.now()
    heart_94_95 = Hilmo_heart_preparation(hilmo_heart_1994_1995, test=True)
    Hilmo_94_95_processing(hilmo_1994_1995, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_94_95, test=True)
    END = datetime.now()
    print(f'hilmo_1994_1995 + heart processing took {(END-START)} hour:min:sec')
    del heart_94_95
    gc.collect() 


    START = datetime.now()
    heart_96_18 = Hilmo_heart_preparation(hilmo_heart_1996_2018, test=True)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_96_18, test=True)
    END = datetime.now()
    print(f'hilmo_1996_2018 + heart processing took {(END-START)} hour:min:sec')	
    del heart_96_18
    gc.collect() 


    START = datetime.now()
    heart_19_21 = Hilmo_heart_preparation(hilmo_heart_2019_2021, test=True)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_19_21, test=True)
    END = datetime.now()
    print(f'hilmo_2019_2021 + heart processing took {(END-START)} hour:min:sec')
    del heart_19_21
    gc.collect() 

# ---

    START = datetime.now()
    oper_96_18 = Hilmo_operations_preparation(hilmo_oper_1996_2018, test=True)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_96_18, test=True)
    END = datetime.now()
    print(f'hilmo_1996_2018 + oper processing took {(END-START)} hour:min:sec')
    del oper_96_18
    gc.collect() 


    START = datetime.now()
    oper_19_21 = Hilmo_operations_preparation(hilmo_oper_2019_2021, test=True)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_19_21, test=True) 
    END = datetime.now()
    print(f'hilmo_2019_2021 + oper processing took {(END-START)} hour:min:sec')
    del oper_19_21
    gc.collect() 

# ---

    START = datetime.now()
    diag_96_18 = Hilmo_diagnosis_preparation(hilmo_diag_1996_2018, test=True)
    Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=diag_96_18, test=True)
    END = datetime.now()
    print(f'hilmo_1996_2018 + diag processing took {(END-START)} hour:min:sec')
    del diag_96_18
    gc.collect() 


    START = datetime.now()
    diag_19_21 = Hilmo_diagnosis_preparation(hilmo_diag_2019_2021, test=True)
    Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=diag_19_21, test=True) 
    END = datetime.now()
    print(f'hilmo_2019_2021 + diag processing took {(END-START)} hour:min:sec')
    del diag_19_21
    gc.collect()  


def preprocess_avohilmo():

    START = datetime.now()  

    icd10_11_16 = AvoHilmo_codes_preparation(avohilmo_icd10_2011_2016, source='icd10', test=True)
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=icd10_11_16, test=True)
    del icd10_11_16
    gc.collect() 
    
    icd10_17_19 = AvoHilmo_codes_preparation(avohilmo_icd10_2017_2019, source='icd10', test=True)
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=icd10_17_19, test=True)
    del icd10_17_19
    gc.collect() 
       
    icd10_20_21 = AvoHilmo_codes_preparation(avohilmo_icd10_2020_2021, source='icd10', test=True)
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=icd10_20_21, test=True)
    del icd10_20_21
    gc.collect() 

    END = datetime.now()
    print(f'avohilmo + icd10 processing took {(END-START)} hour:min:sec')

# ---

    START = datetime.now()		

    icpc2_11_16 = AvoHilmo_codes_preparation(avohilmo_icpc2_2011_2016, source='icpc2', test=True)
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=icpc2_11_16, test=True)
    del icpc2_11_16
    gc.collect() 
    
    icpc2_17_19 = AvoHilmo_codes_preparation(avohilmo_icpc2_2017_2019, source='icpc2', test=True)
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=icpc2_17_19, test=True)	
    del icpc2_17_19
    gc.collect() 
    
    icpc2_20_21 = AvoHilmo_codes_preparation(avohilmo_icpc2_2020_2021, source='icpc2', test=True)
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=icpc2_20_21, test=True)	
    del icpc2_20_21
    gc.collect() 

    END = datetime.now()
    print(f'avohilmo + icpc2 processing took {(END-START)} hour:min:sec')

# ---

    START = datetime.now()	

    oral_11_16 = AvoHilmo_codes_preparation(avohilmo_oral_2011_2016, source='oral', test=True)
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]    
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oral_11_16, test=True)
    del oral_11_16
    gc.collect() 
    
    oral_17_19 = AvoHilmo_codes_preparation(avohilmo_oral_2017_2019, source='oral', test=True)
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oral_17_19, test=True)
    del oral_17_19
    gc.collect() 
    
    oral_20_21 = AvoHilmo_codes_preparation(avohilmo_oral_2020_2021, source='oral', test=True)
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oral_20_21, test=True)
    del oral_20_21
    gc.collect() 

    END = datetime.now()
    print(f'avohilmo + oral processing took {(END-START)} hour:min:sec')

# ---

    START = datetime.now()	

    oper_11_16 = AvoHilmo_codes_preparation(avohilmo_oper_2011_2016, source='oper', test=True)
    avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_11_16, test=True)	
    del oper_11_16
    gc.collect() 
    
    oper_17_19 = AvoHilmo_codes_preparation(avohilmo_oper_2017_2019, source='oper', test=True)
    avohilmo_to_process = [avohilmo_2017_2018,avohilmo_2019_2020]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_17_19, test=True)	
    del oper_17_19
    gc.collect() 
    
    oper_20_21 = AvoHilmo_codes_preparation(avohilmo_oper_2020_2021, source='oper', test=True)
    avohilmo_to_process = [avohilmo_2020,avohilmo_2021]
    for avohilmo in avohilmo_to_process:
        AvoHilmo_processing(avohilmo, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_20_21, test=True)
    del oper_20_21
    gc.collect() 	

    END = datetime.now()
    print(f'avohilmo + oper processing took {(END-START)} hour:min:sec')


def preprocess_death():
    START = datetime.now()
    DeathRegistry_processing(death_pre2020,DOB_map=DOB_map, test=True)
    DeathRegistry_processing(death_2020_2021,DOB_map=DOB_map, test=True)
    END = datetime.now()
    print(f'death registry processing took {(END-START)} hour:min:sec')


def preprocess_cancer():
    START = datetime.now()
    CancerRegistry_processing(cancer,DOB_map=DOB_map, test=True)
    END = datetime.now()
    print(f'cancer registry processing took {(END-START)} hour:min:sec')


def preprocess_kela_reimbursement():
    START = datetime.now()
    KelaReimbursement_PRE20_processing(kela_reimbursement_pre2020, DOB_map=DOB_map, test=True)
    KelaReimbursement_20_21_processing(kela_reimbursement_2020_2021, DOB_map=DOB_map, test=True)
    END = datetime.now()
    print(f'kela reimbursement processing took {(END-START)} hour:min:sec')


def preprocess_kela_purchases():
    START = datetime.now()
    for purchase_file in kela_purchase_pre2020:
        KelaPurchase_PRE20_processing(purchase_file,DOB_map=DOB_map, test=True)
    for purchase_file in kela_purchase_2020_2021:
        KelaPurchase_20_21_processing(purchase_file,DOB_map=DOB_map, test=True)
    END = datetime.now()
    print(f'kela purchases processing took {(END-START)} hour:min:sec')


if __name__ == '__main__':

    p_hilmo = multiprocessing.Process(target=preprocess_hilmo)
    p_avohilmo = multiprocessing.Process(target=preprocess_avohilmo)
    p_death = multiprocessing.Process(target=preprocess_death)
    p_cancer = multiprocessing.Process(target=preprocess_cancer)
    p_kela_reimbursement = multiprocessing.Process(target=preprocess_kela_reimbursement)
    p_kela_purchases = multiprocessing.Process(target=preprocess_kela_purchases)

    # Start multiprocessing
    p_hilmo.start()
    p_avohilmo.start()
    p_death.start()
    p_cancer.start()
    p_kela_reimbursement.start()
    p_kela_purchases.start()

    # Wait for all the processes to end
    p_hilmo.join()
    p_avohilmo.join()
    p_death.join()
    p_cancer.join()
    p_kela_reimbursement.join()
    p_kela_purchases.join()

    print("test finished!") 
