
# LIBRARIES

import re
import pandas as pd
import numpy as np

# CONSTANTS

DAYS_TO_YEARS = 365.24

#---------------------
# REGISTRY-SPECIFIC FUNCTIONS

def Hilmo_69_86_preparation(file_path:str, file_sep:str):

	# fetch data
	data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	# define new columns
	data['ICDVER'] 		= 8
	data['TULOPVM'] 	= pd.to_datetime( data['TULOPV'].str.slice(stop=10), format='%d.%m.%Y' )
	data['LAHTOPVM']	= pd.to_datetime( data['LAHTOPV'].str.slice(stop=10), format='%d.%m.%Y' )
	# rename columns
	data.rename( 
		columns = {
		'DG1':'PDGO',
		'DG2':'SDG10',
		'DG3':'SDG20',
		'DG4':'SDG30'
		},
		inplace=True)

	return data



def Hilmo_87_93_preparation(file_path:str, file_sep:str):

	# fetch data
	data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	# define new columns
	data['ICDVER'] 		= 9
	data['TULOPVM'] 	= pd.to_datetime( data['TUPVA'].str.slice(stop=10), format='%d.%m.%Y' )
	data['LAHTOPVM'] 	= pd.to_datetime( data['LPVM'].str.slice(stop=10), format='%d.%m.%Y' )
	# rename columns
	data.rename( 
		columns = {
		'PDG' :'PDGO',
		'SDG1':'SDG10',
		'SDG2':'SDG20',
		'SDG3':'SDG30'
		},
		inplace=True)

	return data



def Hilmo_94_95_preparation(file_path:str, file_sep:str):

	# fetch data
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# define new columns
	data['ICDVER'] 		= 9
	data['TULOPVM'] 	= pd.to_datetime( data['TUPVA'].str.slice(stop=10), format='%d.%m.%Y' )
	data['LAHTOPVM'] 	= pd.to_datetime( data['LPVM'].str.slice(stop=10), format='%d.%m.%Y' )
	# rename columns
	data.rename( 
		columns = {
		'PDG' :'PDGO',
		'SDG1':'PDG1O',
		'SDG2':'SDG20',
		'TMPTYP1':'TPTYP1',
		'TMPTYP2':'TPTYP2',
		'TMPTYP3':'TPTYP3'
		},
		inplace=True)

	return data



def Hilmo_pre95_preparation(hilmo_pre95):

	# add date of birth
	DOB_map = pd.read_csv('/data/original_data/dvv/Finregistry_IDs_and_full_DOB.txt',sep = ';', encoding='latin-1')
	NewData = hilmo_pre95.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['EVENT_AGE'] = round( (NewData.TULOPVM - FinalData.SYNTPVM).days/DAYS_TO_YEARS, 2)	

	#remove missing avalues
	hilmo_pre95_agecheck = NewData.loc[ !NewData.EVENT_AGE.isna() ]
	return hilmo_pre95_agecheck




def DeathRegistry_preparation(file_path:str, file_sep:str):
	
	# fetch data
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	DOB_map = pd.read_csv('/data/original_data/dvv/Finregistry_IDs_and_full_DOB.txt',sep = ';', encoding='latin-1')
	NewData = data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	# TODO
	...
	NewData['ICDVER'] = 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 

	return ...


def CancerRegistry_preparation(file_path:str, file_sep:str):
	
	# fetch data
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	DOB_map = pd.read_csv('/data/original_data/dvv/Finregistry_IDs_and_full_DOB.txt',sep = ';', encoding='latin-1')
	NewData = data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['dg_date']			= pd.to_datetime( data['dg_date'], format='%Y-%m-%d' )
	NewData['EVENT_AGE'] 		= round( (NewData.TULOPVM - FinalData.SYNTPVM).days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YEAR'] 		= NewData.dg_date.year	
	NewData['ICDVER'] 			= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	NewData['MY_CANC_COD_TOPO'] = np.NaN
	NewData['MY_CANC_COD_AGE'] 	= np.NaN
	NewData['MY_CANC_COD_YEAR'] = np.NaN

	#remove missing values
	cancer_agecheck = NewData.loc[ !NewData.EVENT_AGE.isna() ]
	return cancer_agecheck



def KelaReimbursement_preparation(file_path:str, file_sep:str):

	# fetch data
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	DOB_map = pd.read_csv('/data/original_data/dvv/Finregistry_IDs_and_full_DOB.txt',sep = ';', encoding='latin-1')
	NewData = data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['LAAKEKORVPVM']	= pd.to_datetime( data['ALPV'], format='%Y-%m-%d' )
	NewData['EVENT_AGE'] 	= round( (NewData.LAAKEKORVPVM - FinalData.SYNTPVM).days/DAYS_TO_YEARS, 2)
	NewData['EVENT_YEAR'] 	= NewData.LAAKEKORVPVM.year
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	#rename columns
	NewData.rename({'DIAG':'ICD', 'SK1':'KELA_DISEASE'}, inplace = True )

	#remove missing values
	reimb_agecheck = NewData.loc[ !NewData.EVENT_AGE.isna() ]
	return reimb_agecheck



def KelaPurchase_preparation(KelaPurchase_files:list):

	# create aggregated dataset
	data = pd.concat(KelaPurchase_files)
	# add date of birth
	DOB_map = pd.read_csv('/data/original_data/dvv/Finregistry_IDs_and_full_DOB.txt',sep = ';', encoding='latin-1')
	NewData = data.merge(DOB_map,left_on = 'HETU',right_on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['LAAKEOSTPVM'] 	= datetime.strptime( AggregatedData['OTPVM'].astype(char), '%d.%m.%Y')
	NewData['EVENT_AGE'] 	= round( (NewData.LAAKEOSTPVM - FinalData.SYNTPVM).days/DAYS_TO_YEARS, 2)
	NewData['EVENT_YEAR'] 	= NewData.LAAKEOSTPVM.year
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	#rename columns
	NewData.rename( 'ATC':'ATC_CODE', inplace = True )

	
	#remove missing values
	purch_agecheck = NewData.loc[ !NewData.EVENT_AGE.isna() ]
	return purch_agecheck
	


