
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry   
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################


import os
import re
import pandas as pd
import numpy as np

# import info on Date_Of_Birth and Date_Of_Death 
minimal_pheno = pd.read_csv('/data/processed_data/minimal_phenotype/minimal_phenotype_2023-05-02.csv',sep = ',', encoding='latin-1')
BIRTH_DEATH_MAP  = min_pheno[:,['FINREGISTRYID','date_of_birth','death_date']]

# import all processing functions
from func import *

# import all file paths
from config import *


if __name__ == '__main__':

	# HILMO
	print('start processing hilmo files')
	Hilmo_69_86_processing(hilmo_1969_1986,DOB_map=BIRTH_DEATH_MAP)
	Hilmo_87_93_processing(hilmo_1987_1993,DOB_map=BIRTH_DEATH_MAP)
	Hilmo_94_95_processing(hilmo_1994_1995,DOB_map=BIRTH_DEATH_MAP)	
	Hilmo_POST95_processing(hilmo_1995_2018)
	Hilmo_POST95_processing(hilmo_2019_2021)

	Hilmo_diagnosis_processing(hilmo_diag_1995_2018)
	Hilmo_diagnosis_processing(hilmo_diag_2019_2021)

	Hilmo_operations_processing(hilmo_oper_1995_2018)
	Hilmo_operations_processing(hilmo_oper_2019_2021)

	Hilmo_heart_processing(hilmo_heart_1994_1995)
	Hilmo_heart_processing(hilmo_heart_1995_2018)
	Hilmo_heart_processing(hilmo_heart_2019_2021)

	# remember to select columns at the end

	# AVOHILMO
	print('start processing avohilmo files')
	AvoHilmo_icd10_processing(avohilmo_icd10_2011_2016)
	AvoHilmo_icd10_processing(avohilmo_icd10_2017_2019)
	AvoHilmo_icd10_processing(avohilmo_icd10_2020_2021)
	#concat everything togheter -> filter year 

	icd10 = pd.concat([...])

	AvoHilmo_icpc2_processing(avohilmo_icpc2_2011_2016)
	AvoHilmo_icpc2_processing(avohilmo_icpc2_2017_2020)
	AvoHilmo_icpc2_processing(avohilmo_icpc2_2020_2021)
	#concat everything togheter

	AvoHilmo_oral_processing(avohilmo_oral_2011_2016)
	AvoHilmo_oral_processing(avohilmo_oral_2017_2019)
	AvoHilmo_oral_processing(avohilmo_oper_2020_2021)
	#concat everything togheter

	AvoHilmo_processing(avohilmo_2011_2012,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_processing(avohilmo_2013_2014,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_processing(avohilmo_2015_2016,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_processing(avohilmo_2017_2018,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_processing(avohilmo_2019_2020,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_processing(avohilmo_2020,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_processing(avohilmo_2021,DOB_map=BIRTH_DEATH_MAP)
	# merge with previous part

	AvoHilmo = foo(icd10.loc[icd10["year"].isin(2011, 2012)])

	# OTHER REGISTRIES
	print('start processing death registry')
	DeathRegistry_processing(death,DOB_map=BIRTH_DEATH_MAP)

	print('start processing cancer registry')
	CancerRegistry_processing(cancer,DOB_map=BIRTH_DEATH_MAP)

	print('start processing kela registry')
	KelaReimbursement_processing(kela_reimburement)
	for purchase_file in kela_purchase_filelist:
		KelaPurchase_processing(purchase_file,DOB_map=BIRTH_DEATH_MAP)


