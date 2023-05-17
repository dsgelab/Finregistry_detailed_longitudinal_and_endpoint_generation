
from preparation_functions import *
from processing_functions  import *
import os

if __name__ == '__main__':

	#---------------------------
	# PREPARATION

	hilmo_6986_prepared 	= Hilmo_69_86_preparation('/data/original_data/thl_hilmo/thl2019_1776_poisto_6986.csv.finreg_IDs',file_sep=';')
	hilmo_8793_prepared 	= Hilmo_87_93_preparation('/data/original_data/thl_hilmo/thl2019_1776_poisto_8793.csv.finreg_IDs',file_sep=';')
	hilmo_9495_prepared 	= Hilmo_94_95_preparation('/data/original_data/thl_hilmo/thl2019_1776_hilmo_9495.csv.finreg_IDs' ,file_sep=';')
	hilmo_pre95 = pd.concat([hilmo_6986_prepared,hilmo_8793_prepared,hilmo_9495_prepared])
	hilmo_pre95_prepared 	= Hilmo_pre95_preparation(hilmo_pre95)

	death_prepared 			= DeathRegistry_preparation()
	cancer_prepared 		= CancerRegistry_preparation('/data/original_data/thl_cancer/fcr_data.csv.finreg_IDs',file_sep=';')

	filelist = #to import from sys print 
	purchases_prepared 		= KelaPurchase_preparation(KelaPurchase_files = filelist)
	reimbursement_prepared 	= KelaReimbursement_preparation('/data/original_data/kela_reimbursement/175_522_2020_LAAKEKORVAUSOIKEUDET.csv.finreg_IDs',file_sep=';')


	#---------------------------
	# PROCESSING



	#---------------------------
	# DETAILED LONGITUDINAL CREATION
