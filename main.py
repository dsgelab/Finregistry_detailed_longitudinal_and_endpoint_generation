
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry   
# LAST UPDATE: 	MAY 2023
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################

from func import *
import os
import re
import pandas as pd
import numpy as np

if __name__ == '__main__':

	# import info on Date_Of_Birth and Date_Of_Death
	DOB_map = pd.read_csv('/data/original_data/dvv/Finregistry_IDs_and_full_DOB.txt',sep = ';', encoding='latin-1')
	DOD_map = pd.read_csv('/data/original_data/dvv/...',sep = ';', encoding='latin-1')

	#---------------------------
	# PREPARATION

	hilmo_6986_prepared 	= Hilmo_69_86_preparation('/data/original_data/thl_hilmo/thl2019_1776_poisto_6986.csv.finreg_IDs',file_sep=';')
	hilmo_8793_prepared 	= Hilmo_87_93_preparation('/data/original_data/thl_hilmo/thl2019_1776_poisto_8793.csv.finreg_IDs',file_sep=';')
	hilmo_9495_prepared 	= Hilmo_94_95_preparation('/data/original_data/thl_hilmo/thl2019_1776_hilmo_9495.csv.finreg_IDs' ,file_sep=';')
	
	hilmo_pre95 = pd.concat([hilmo_6986_prepared,hilmo_8793_prepared,hilmo_9495_prepared])
	hilmo_pre95_prepared 	= Hilmo_PRE95_preparation(hilmo_pre95=hilmo_pre95,DOB_map=DOB_map)

	# HILMO
	# to run diagnosis , operations , heart and then join together
	# also remember to select columns at the end

	# AVOHILMO
	# to run icd10 , icpc2 , oral , operation and join together

	death_prepared 			= DeathRegistry_preparation('/data/original_data/sf_death/thl2019_1776_ksyy_tutkimus.csv.finreg_IDs',file_sep=';',DOB_map=DOB_map)
	cancer_prepared 		= CancerRegistry_preparation('/data/original_data/thl_cancer/fcr_data.csv.finreg_IDs',file_sep=';',DOB_map=DOB_map)

	filelist = #to import from sys print 
	for f in filelist:
		kela_prepared = KelaPurchase_preparation(f,sep = ';',DOB_map=DOB_map)
		# write-out the file/concat
	reimbursement_prepared 	= KelaReimbursement_preparation('/data/original_data/kela_reimbursement/175_522_2020_LAAKEKORVAUSOIKEUDET.csv.finreg_IDs',file_sep=';')


	#---------------------------
	# DETAILED LONGITUDINAL CREATION
