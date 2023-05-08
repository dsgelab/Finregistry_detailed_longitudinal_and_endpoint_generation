
# PREPROCESSING FOR DETAILED LONGITUDINAL

# second part of processing, check script_1.py for first part

# LIBRARIES

import os
import string
from datetime import datetime
import pandas as pd
import numpy as np

#--------------------
# UTILITY FUNCTIONS

def htun2date(ht):
	pass()


#-------------------


def KelaPurchasePreprocessing():
	# NB: data used was created here: script_1.py
	data = ...

	# remove duplicates ?

	# missing values
	NewData.loc[NewData==''] = np.NaN

	# add columns
	NewData['SOURCE'] 			= 'PURCH'
	NewData['CATEGORY'] 		= np.NaN
	NewData['ICDVER']			= np.NaN
	NewData['EVENT_YRMNTH']	= data['LAAKEOSTPVM'][:7]

