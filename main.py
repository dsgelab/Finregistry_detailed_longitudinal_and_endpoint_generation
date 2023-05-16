
from preparation_functions.py import *
from processing_functions.py  import *

if __name__ == '__main__':

	# prepare the hilmo datasets to be joined
	hilmo_6986_prepared = Hilmo_69_86_preparation('/data/original_data/thl_hilmo/thl2019_1776_poisto_6986.csv.finreg_IDs',file_sep=';')
	hilmo_8793_prepared = Hilmo_87_93_preparation('/data/original_data/thl_hilmo/thl2019_1776_poisto_8793.csv.finreg_IDs',file_sep=';')
	hilmo_9495_prepared = Hilmo_94_95_preparation('/data/original_data/thl_hilmo/thl2019_1776_hilmo_9495.csv.finreg_IDs' ,file_sep=';')
	
	# create the hilmo_pre95 dataset
	hilmo_pre95 = pd.concat([hilmo_6986_prepared,hilmo_8793_prepared,hilmo_9495_prepared])
	hilmo_pre95_prepared = Hilmo_pre95_preparation(hilmo_pre95)