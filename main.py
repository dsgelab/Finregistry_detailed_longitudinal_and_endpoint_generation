
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry   
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################

from func import *
import os
import re
import pandas as pd
import numpy as np

# import info on Date_Of_Birth and Date_Of_Death 
minimal_pheno = pd.read_csv('/data/processed_data/minimal_phenotype/minimal_phenotype_2023-05-02.csv',sep = ',', encoding='latin-1')
BIRTH_DEATH_MAP  = min_pheno[:,['FINREGISTRYID','date_of_birth','death_date']]

# DEFINE PATHS (in Finregistry):

THL_HILMO_PATH = '/data/original_data/thl_hilmo/'

hilmo_1969_1986 			= THL_HILMO_PATH+'thl2019_1776_poisto_6986.csv.finreg_IDs'
hilmo_1987_1993 			= THL_HILMO_PATH+'thl2019_1776_poisto_8793.csv.finreg_IDs'
hilmo_1994_1995  			= THL_HILMO_PATH+'thl2019_1776_hilmo_9495.csv.finreg_IDs' 
hilmo_1995_2018  			= THL_HILMO_PATH+'thl2019_1776_hilmo.csv.finreg_IDs' 
hilmo_2019_2021  			= THL_HILMO_PATH+'THL2021_2196_HILMO_2019_2021.csv.finreg_IDs' 

hilmo_diag_1995_2018 		= THL_HILMO_PATH+'thl2019_1776_hilmo_diagnoosit_kaikki.csv.finreg_IDs'
hilmo_diag_2019_2021		= THL_HILMO_PATH+'THL2021_2196_HILMO_DIAG.csv.finreg_IDs'

hilmo_oper_1995_2018		= THL_HILMO_PATH+'thl2019_1776_hilmo_toimenpide.csv.finreg_IDs'
hilmo_oper_2019_2021		= THL_HILMO_PATH+'THL2021_2196_HILMO_TOIMP.csv.finreg_IDs'

hilmo_heart_1994_1995		= THL_HILMO_PATH+'thl2019_1776_hilmo_9495_syp.csv.finreg_IDs '
hilmo_heart_1995_2018		= THL_HILMO_PATH+'thl2019_1776_hilmo_syp.csv.finreg_IDs'
hilmo_heart_2019_2021		= THL_HILMO_PATH+'THL2021_2196_HILMO_SYP.csv.finreg_IDs'

THL_AVOHILMO_PATH = '/data/original_data/thl_avohilmo/'

avohilmo_icd10_2011_2016	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_icd10.csv.finreg_IDs'
avohilmo_icd10_2017_2019	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_icd10.csv.finreg_IDs'
avohilmo_icd10_2020_2021	= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_ICD10_DIAG.csv.finreg_IDs'

avohilmo_icpc2_2011_2016	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_icpc2.csv.finreg_IDs'
avohilmo_icpc2_2017_2020	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_icpc2.csv.finreg_IDs '
avohilmo_icpc2_2020_2021	= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_ICPC2_DIAG.csv.finreg_IDs'

avohilmo_oral_2011_2016		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_suu_toimenpide.csv.finreg_IDs'
avohilmo_oral_2017_2019		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_suu_toimp.csv.finreg_IDs'
avohilmo_oral_2020_2021		= THL_AVOHILMO_PATH+'thl2021_2196_avohilmo_suu_toimp.csv.finreg_IDs'

avohilmo_oper_2011_2016		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_toimenpide.csv.finreg_IDs'
avohilmo_oper_2017_2019		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_toimenpide.csv.finreg_IDs'
avohilmo_oper_2020_2021		= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_TOIMP.csv.finreg_IDs'

avohilmo_2011_2012 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_11_12.csv.finreg_IDs'
avohilmo_2013_2014 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_13_14.csv.finreg_IDs'
avohilmo_2015_2016  		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_15_16.csv.finreg_IDs'
avohilmo_2017_2018 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_18.csv.finreg_IDs'
avohilmo_2019_2020 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_19_20.csv.finreg_IDs'
avohilmo_2020  				= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_2020.csv.finreg_IDs'
avohilmo_2021  				= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_2021.csv.finreg_IDs'

death 						= '/data/original_data/sf_death/thl2019_1776_ksyy_tutkimus.csv.finreg_IDs'

cancer 						= '/data/original_data/thl_cancer/fcr_data.csv.finreg_IDs'

kela_reimburement 			= '/data/original_data/kela_reimbursement/175_522_2020_LAAKEKORVAUSOIKEUDET.csv.finreg_IDs'
kela_purchase_filelist		= ['/data/original_data/kela_purchase/175_522_2020_LAAKEOSTOT_'+str(n)+'.csv.finreg_IDs' for n in range(1995,2020)]



if __name__ == '__main__':

	#---------------------------
	# PREPARATION

	# HILMO
	hilmo_6986_prepared 		= Hilmo_69_86_preparation(hilmo_1969_1986,DOB_map=BIRTH_DEATH_MAP)
	hilmo_8793_prepared 		= Hilmo_87_93_preparation(hilmo_1987_1993,DOB_map=BIRTH_DEATH_MAP)
	hilmo_9495_prepared 		= Hilmo_94_95_preparation(hilmo_1994_1995,DOB_map=BIRTH_DEATH_MAP)	
	hilmo_9518_prepared 		= Hilmo_POST95_preparation(hilmo_1995_2018)
	hilmo_1921_prepared 		= Hilmo_POST95_preparation(hilmo_2019_2021)

	hilmo_9518_diag_prepared 	= Hilmo_diagnosis_preparation(hilmo_diag_1995_2018)
	hilmo_1921_diag_prepared	= Hilmo_diagnosis_preparation(hilmo_diag_2019_2021)

	hilmo_9518_oper_prepared 	= Hilmo_operations_preparation(hilmo_oper_1995_2018)
	hilmo_1921_oper_prepared	= Hilmo_operations_preparation(hilmo_oper_2019_2021)

	hilmo_9495_heart_prepared 	= Hilmo_heart_preparation(hilmo_heart_1994_1995)
	hilmo_9518_heart_prepared 	= Hilmo_heart_preparation(hilmo_heart_1995_2018)
	hilmo_1921_heart_prepared	= Hilmo_heart_preparation(hilmo_heart_2019_2021)

	# remember to select columns at the end

	# AVOHILMO
	AvoHilmo_icd10_preparation(avohilmo_icd10_2011_2016)
	AvoHilmo_icd10_preparation(avohilmo_icd10_2017_2019)
	AvoHilmo_icd10_preparation(avohilmo_icd10_2020_2021)
	#concat everything togheter

	AvoHilmo_icpc2_preparation(avohilmo_icpc2_2011_2016)
	AvoHilmo_icpc2_preparation(avohilmo_icpc2_2017_2020)
	AvoHilmo_icpc2_preparation(avohilmo_icpc2_2020_2021)
	#concat everything togheter

	AvoHilmo_oral_preparation(avohilmo_oral_2011_2016)
	AvoHilmo_oral_preparation(avohilmo_oral_2017_2019)
	AvoHilmo_oral_preparation(avohilmo_oper_2020_2021)
	#concat everything togheter

	AvoHilmo_preparation(avohilmo_2011_2012,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_preparation(avohilmo_2013_2014,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_preparation(avohilmo_2015_2016,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_preparation(avohilmo_2017_2018,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_preparation(avohilmo_2019_2020,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_preparation(avohilmo_2020,DOB_map=BIRTH_DEATH_MAP)
	AvoHilmo_preparation(avohilmo_2021,DOB_map=BIRTH_DEATH_MAP)
	# merge with previous part

	# OTHER REGISTRIES
	death_prepared 				= DeathRegistry_preparation(death,DOB_map=BIRTH_DEATH_MAP)
	cancer_prepared 			= CancerRegistry_preparation(cancer,DOB_map=BIRTH_DEATH_MAP)
	reimbursement_prepared 		= KelaReimbursement_preparation(kela_reimburement)
	for purchase_file in kela_purchase_filelist:
		purchase_prepared 		= KelaPurchase_preparation(purchase_file,DOB_map=BIRTH_DEATH_MAP)
		# write-out the file/concat



	#---------------------------
	# DETAILED LONGITUDINAL CREATION


