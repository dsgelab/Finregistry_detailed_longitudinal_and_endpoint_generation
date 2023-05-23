
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry   
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################

from func import *
import os
import re
import pandas as pd
import numpy as np


# DEFINE PATHS (in Finregistry):
THL_HILMO_PATH = '/data/original_data/thl_hilmo/'
poisto_1969_1986 		= THL_HILMO_PATH+'thl2019_1776_poisto_6986.csv.finreg_IDs'
poisto_1987_1993 		= THL_HILMO_PATH+'thl2019_1776_poisto_8793.csv.finreg_IDs'
hilmo_1994_1995  		= THL_HILMO_PATH+'thl2019_1776_hilmo_9495.csv.finreg_IDs' 
hilmo_1995_2018  		= THL_HILMO_PATH+'thl2019_1776_hilmo.csv.finreg_IDs' 
hilmo_2019_2021  		= THL_HILMO_PATH+'THL2021_2196_HILMO_2019_2021.csv.finreg_IDs' 
hilmo_diag_1995_2018 	= THL_HILMO_PATH+'thl2019_1776_hilmo_diagnoosit_kaikki.csv.finreg_IDs'
hilmo_diag_2019_2021	= THL_HILMO_PATH+'THL2021_2196_HILMO_DIAG.csv.finreg_IDs'
hilmo_oper_1995_2018	= THL_HILMO_PATH+'thl2019_1776_hilmo_toimenpide.csv.finreg_IDs'
hilmo_oper_2019_2021	= THL_HILMO_PATH+'THL2021_2196_HILMO_TOIMP.csv.finreg_IDs'
hilmo_heart_1994_1995	= THL_HILMO_PATH+'thl2019_1776_hilmo_9495_syp.csv.finreg_IDs '
hilmo_heart_1995_2018	= THL_HILMO_PATH+'thl2019_1776_hilmo_syp.csv.finreg_IDs'
hilmo_heart_2019_2021	= THL_HILMO_PATH+'THL2021_2196_HILMO_SYP.csv.finreg_IDs'

# to add all avohilmo
THL_AVOHILMO_PATH = '/data/original_data/thl_avohilmo/'

death 					= '/data/original_data/sf_death/thl2019_1776_ksyy_tutkimus.csv.finreg_IDs'
cancer 					= '/data/original_data/thl_cancer/fcr_data.csv.finreg_IDs'
kela_reimburement 		= '/data/original_data/kela_reimbursement/175_522_2020_LAAKEKORVAUSOIKEUDET.csv.finreg_IDs'

# import info on Date_Of_Birth and Date_Of_Death
DOB_map = pd.read_csv('/data/original_data/dvv/Finregistry_IDs_and_full_DOB.txt',sep = ';', encoding='latin-1')
DOD_map = pd.read_csv('/data/original_data/dvv/...',sep = ';', encoding='latin-1')

if __name__ == '__main__':

	#---------------------------
	# PREPARATION

	hilmo_6986_prepared 		= Hilmo_69_86_preparation(poisto_1969_1986,DOB_map=DOB_map)
	hilmo_8793_prepared 		= Hilmo_87_93_preparation(poisto_1987_1993,DOB_map=DOB_map)
	hilmo_9495_prepared 		= Hilmo_94_95_preparation(hilmo_1994_1995,DOB_map=DOB_map)	
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
	# to run icd10 , icpc2 , oral 

	death_prepared 			= DeathRegistry_preparation(death,file_sep=';',DOB_map=DOB_map)
	cancer_prepared 		= CancerRegistry_preparation(cancer,file_sep=';',DOB_map=DOB_map)

	filelist = #to import from sys print 
	for purchase_file in filelist:
		purchase_prepared = KelaPurchase_preparation(purchase_file,sep = ';',DOB_map=DOB_map)
		# write-out the file/concat
		
	reimbursement_prepared 	= KelaReimbursement_preparation(kela_reimburement,file_sep=';')


	#---------------------------
	# DETAILED LONGITUDINAL CREATION
