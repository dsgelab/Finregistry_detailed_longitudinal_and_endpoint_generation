
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry 2023  
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################


import os
import re
import pandas as pd
import numpy as np

# import info on Date_Of_Birth and Date_Of_Death 
BIRTH_DEATH_MAP = pd.read_csv('/data/processed_data/minimal_phenotype/minimal_phenotype_2023-05-02.csv',sep = ',', encoding='latin-1')
BIRTH_DEATH_MAP = BIRTH_DEATH_MAP[['FINREGISTRYID','date_of_birth','death_date']]

# import all processing functions
from func import *

# import all file paths
from config import *


##########################################################
# CREATE DETAILED LONGITUDINAL 

if __name__ == '__main__':

	# HILMO
	print('start processing hilmo files')
	Hilmo_69_86_processing(hilmo_1969_1986, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_87_93_processing(hilmo_1987_1993, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_94_95_processing(hilmo_1994_1995, DOB_map=BIRTH_DEATH_MAP)	
	Hilmo_POST95_processing(hilmo_1995_2018, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_POST95_processing(hilmo_2019_2021, DOB_map=BIRTH_DEATH_MAP)

	Hilmo_diagnosis_processing(hilmo_diag_1995_2018)
	Hilmo_diagnosis_processing(hilmo_diag_2019_2021)

	Hilmo_operations_processing(hilmo_oper_1995_2018, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_operations_processing(hilmo_oper_2019_2021, DOB_map=BIRTH_DEATH_MAP)

	Hilmo_heart_processing(hilmo_heart_1994_1995)
	Hilmo_heart_processing(hilmo_heart_1995_2018)
	Hilmo_heart_processing(hilmo_heart_2019_2021)

	# AVOHILMO
	print('start processing avohilmo files')
	icd10_11_16 = AvoHilmo_icd10_preparation(avohilmo_icd10_2011_2016)
	icd10_17_19 = AvoHilmo_icd10_preparation(avohilmo_icd10_2017_2019)
	icd10_20_21 = AvoHilmo_icd10_preparation(avohilmo_icd10_2020_2021)
	#concat everything togheter 
	icd10 = pd.concat([icd10_11_16,icd10_17_19,icd10_20_21])

	icpc2_11_16 = AvoHilmo_icpc2_preparation(avohilmo_icpc2_2011_2016)
	icpc2_17_19 = AvoHilmo_icpc2_preparation(avohilmo_icpc2_2017_2019)
	icpc2_20_21 = AvoHilmo_icpc2_preparation(avohilmo_icpc2_2020_2021)
	#concat everything togheter
	icpc2 = pd.concat([icpc2_11_16,icpc2_17_19,icpc2_20_21])

	oral_11_16 = AvoHilmo_oral_preparation(avohilmo_oral_2011_2016)
	oral_17_19 = AvoHilmo_oral_preparation(avohilmo_oral_2017_2019)
	oral_20_21 = AvoHilmo_oral_preparation(avohilmo_oral_2020_2021)
	#concat everything togheter
	oral = pd.concat([oral_11_16,oral_17_19,oral_20_21])

	oper_11_16 = AvoHilmo_operations_preparation(avohilmo_oper_2011_2016)
	oper_17_19 = AvoHilmo_operations_preparation(avohilmo_oper_2017_2019)
	oper_20_21 = AvoHilmo_operations_preparation(avohilmo_oper_2020_2021)
	#concat everything togheter
	oper = pd.concat([oper_11_16,oper_17_19,oper_20_21])

	# merge to main avohilmo files and push to detailed longitudinal
	print('starting avohilmo merging for loop')
	avohilmo_to_merge = [icd10,icpc2,oral,oper]
	avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016,avohilmo_2017_2018,avohilmo_2019_2020,avohilmo_2020,avohilmo_2021]
	for avohilmo in avohilmo_to_process:
		for df in avohilmo_to_merge:
			AvoHilmo_processing(avohilmo, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=df)

	# OTHER REGISTRIES
	print('start processing death registry')
	DeathRegistry_processing(death,DOB_map=BIRTH_DEATH_MAP)

	print('start processing cancer registry')
	CancerRegistry_processing(cancer,DOB_map=BIRTH_DEATH_MAP)

	print('start processing kela registry')
	KelaReimbursement_PRE20_processing(kela_reimbursement_pre2020)
	KelaReimbursement_20_21_processing(kela_reimbursement_2020_2021)

	for purchase_file in kela_purchase_filelist:
		KelaPurchase_processing(purchase_file,DOB_map=BIRTH_DEATH_MAP)


