
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry   
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################

# LIBRARIES

import re
import pandas as pd
import numpy as np

from config import DETAILED_LONGITUDINAL_PATH


##########################################################
# UTILITY VARIABLES

DAYS_TO_YEARS = 365.24

COLUMNS_2_KEEP = [
	"FINREGISTRYID",
	"SOURCE",
	"ICDVER",
	"CATEGORY",
	"INDEX",
	"EVENT_AGE", 
	"EVENT_YRMNTH", 
	"PVM", 
	"CODE1", 
	"CODE2", 
	"CODE3", 
	"CODE4", 
	"CODE5",
	"CODE6", 
	"CODE7"]



##########################################################
# UTILITY FUNCTIONS

def Write2DetailedLongitudinal(Data, path=DETAILED_LONGITUDINAL_PATH):
	Data.to_csv(path_or_buf=path, mode="a", sep=';', encoding='latin-1')

def Write2TestFile(Data, path=TEST_FILE_PATH):
	#NB: replace existing file if there is one
	Data.to_csv(path_or_buf=TEST_FILE_PATH, mode="w", sep=';', encoding='latin-1')


def SpecialCharacterSplit(Data):

	#------------------
	# RULE: if CODE1 has a '*' then ?!

	#------------------
	# RULE: if CODE1 has a '+' then first part goes to CODE2 and second to CODE1

	Data['IS_PLUS'] = Data.CODE1.str.match('+')
	Data_tosplit 	= Data_tosplit.loc[Data_tosplit['IS_PLUS'] == True]
	part1, part2 	= Data_tosplit['CODE1'].str.split('\\+')

	Data.loc[Data.IS_PLUS == True,'CODE2'] = part1
	Data.loc[Data.IS_PLUS == True,'CODE1'] = part2

	#------------------
	# RULE: if CODE1 has a '#' then first part goes to CODE1 and second to CODE3

	Data['IS_HAST']	= Data.CODE1.str.match('+')
	Data_tosplit 	= Data_tosplit.loc[Data_tosplit['IS_HAST'] == True]
	part1, part2 	= Data_tosplit['CODE1'].str.split('\\+')

	Data.loc[Data['IS_HAST'] == True,'CODE1'] = part1
	Data.loc[Data['IS_HAST'] == True,'CODE3'] = part2

	#------------------
	# RULE: if CODE1 has a '&' then first part goes to CODE1 and second to CODE2

	Data['IS_AND'] 	= Data.CODE1.str.match('+')
	Data_tosplit 	= Data_tosplit.loc[Data_tosplit['IS_AND'] == True]
	part1, part2 	= Data_tosplit['CODE1'].str.split('\\+')

	Data.loc[Data['IS_AND'] == True,'CODE1'] = part1
	Data.loc[Data['IS_AND'] == True,'CODE2'] = part2

	return Data	


def Hilmo_DefineOutpat(hilmo):

	# define OUTPAT after 2018
	hilmo.loc[ !( hilmo.TULOPVM.year>2018 & hilmo.YHTEYSTAPA=='R80' ),'SOURCE'] = 'OUTPAT'
	hilmo.loc[ !( hilmo.TULOPVM.year>2018 & hilmo.YHTEYSTAPA=='R10' & hilmo.PALA in [1,3,4,5,6,7,8,31] ),'SOURCE'] = 'OUTPAT'
	hilmo.loc[ !( hilmo.TULOPVM.year>2018 & hilmo.YHTEYSTAPA=='' 	& hilmo.PALA in [1,3,4,5,6,7,8,31] ),'SOURCE'] = 'OUTPAT'

	# define OUTPAT before 2018
	hilmo.loc[ hilmo.TULOPVM.year<=2018 & hilmo.PALA.isna(),'SOURCE'] = 'OUTPAT'
	hilmo.loc[ hilmo.TULOPVM.year<=2018 & hilmo.PALA not in [1,3,4,5,6,7,8,31],'SOURCE'] = 'OUTPAT'

	# NB: all years before 1998 are INPAT (QC)
	hilmo.loc[ hilmo.TULOPVM.year<1998,'SOURCE'] = 'INPAT'

	return hilmo


##########################################################
# REGISTRY-SPECIFIC FUNCTIONS

def Hilmo_69_86_processing(file_path:str, file_sep=';',DOB_map, test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM,    format='%Y-%m-%d',errors='coerce' )
	Data['DEATH_DATE'] 		= pd.to_datetime( Data.death_date, format='%Y-%m-%d',errors='coerce' )
	# format date columns (patient in and out dates)
	Data['TULOPVM'] 		= pd.to_datetime( Data.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	Data['LAHTOPVM']		= pd.to_datetime( Data.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )

	# check that event is after death
	Data.loc[Data.TULOPVM > Data.DEATH_DATE,'TULOPVM'] = Data.DEATH_DATE

	# define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.TULOPVM - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	Data['EVENT_YRMNTH']	= Data.TULOPVM.dt.strftime('%Y-%m')
	Data['INDEX'] 			= np.arange(Data.shape[0]) + 1
	Data['SOURCE'] 			= 'INPAT'
	Data['ICDVER'] 			= 8
	Data['CODE2']			= np.NaN
	Data['CODE3']			= np.NaN
	Data['CODE4']			= (Data.LAHTOPVM - Data.TULOPVM).dt.days

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names

	# perform the reshape
	CATEGORY_DICTIONARY	= {
		'DG1':'0',
		'DG2':'1',
		'DG3':'2',
		'DG4':'3'}
	VAR_FOR_RESHAPE = CATEGORY_DICTIONARY.keys()

	ReshapedData = pd.melt(Data[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final dataset
	VAR_NOT_FOR_RESHAPE = Data.columns[ Data.columns != VAR_FOR_RESHAPE ]
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	Data = Hilmo_DefineOutpat(Data)

	# define OPER_IN
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	Data.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data = Data.loc[ !Data.EVENT_AGE.isna() ]
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 
	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	Data.loc[Data.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_87_93_processing(file_path:str, file_sep=';',DOB_map, test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM.str,    format='%Y-%m-%d', errors='coerce' )
	Data['DEATH_DATE'] 		= pd.to_datetime( Data.death_date.str, format='%Y-%m-%d', errors='coerce')
	# format date columns (patient in and out dates)
	Data['TULOPVM'] 		= pd.to_datetime( Data.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	Data['LAHTOPVM']		= pd.to_datetime( Data.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )

	# check that event is after death
	Data.loc[Data.TULOPVM > Data.DEATH_DATE,'TULOPVM'] = Data.DEATH_DATE

	# define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.TULOPVM - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	Data['EVENT_YRMNTH']	= Data.TULOPVM.dt.strftime('%Y-%m')
	Data['INDEX'] 			= np.arange(Data.shape[0]) + 1
	Data['SOURCE'] 			= 'INPAT'
	Data['ICDVER'] 			= 9
	Data['CODE3']			= np.NaN
	Data['CODE4']			= (Data.LAHTOPVM - Data.TULOPVM).dt.days

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names  

	# rename categories 
	column_names 	= Data.columns
	# FULL RENAME
	new_names		= [s.replace('PDG','0') for s in column_names]
	new_names		= [s.replace('EDIA','EX') for s in new_names]
	new_names		= [s.replace('MTMP1K1','NOM4') for s in new_names]
	new_names		= [s.replace('MTMP2K1','NOM5') for s in new_names]
	# PREFIX RENAME
	new_names		= [s.replace('SDG','') for s in new_names]
	new_names 		= [s.replace('PTMPK','NOM') for s in new_names]
	new_names 		= [s.replace('TMP','MFHL') for s in new_names]
	new_names 		= [s.replace('TP','SFHL') for s in new_names]
	new_names 		= [s.replace('TPTYP','HPO') for s in new_names]
	new_names 		= [s.replace('TMPC','HPN') for s in new_names]
	Data.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = Data.columns[ Data.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(Data[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = Data.columns[ Data.columns != VAR_FOR_RESHAPE ]
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	Data = Hilmo_DefineOutpat(Data)

	# define OPER_IN
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	Data.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data = Data.loc[ !Data.EVENT_AGE.isna() ]
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 
	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	Data.loc[Data.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_94_95_processing(file_path:str, file_sep=';',DOB_map, test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM.str,    format='%Y-%m-%d', errors='coerce' )
	Data['DEATH_DATE'] 		= pd.to_datetime( Data.death_date.str, format='%Y-%m-%d', errors='coerce' )
	# format date columns (patient in and out dates)
	Data['TULOPVM'] 		= pd.to_datetime( Data.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	Data['LAHTOPVM']		= pd.to_datetime( Data.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )
	
	# check that event is after death
	Data.loc[Data.TULOPVM > Data.DEATH_DATE,'TULOPVM'] = Data.DEATH_DATE

	# define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.TULOPVM - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	Data['EVENT_YRMNTH']	= Data.TULOPVM.dt.strftime('%Y-%m')
	Data['INDEX'] 			= np.arange(Data.shape[0] ) + 1
	Data['SOURCE'] 			= 'INPAT'
	Data['CODE3']			= np.NaN
	Data['CODE4']			= (Data.LAHTOPVM - Data.TULOPVM).dt.days
	Data['ICDVER'] 			= 9

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names  

	# rename categories
	column_names 	= Data.columns
	new_names		= [s.replace('PDG','0') for s in column_names]
	new_names		= [s.replace('EDIA','EX') for s in new_names]
	new_names		= [s.replace('SDG','') for s in new_names]
	new_names 		= [s.replace('PTMPK','NOM') for s in new_names]
	new_names		= [s.replace('MTMP1K1','NOM4') for s in new_names]
	new_names		= [s.replace('MTMP2K1','NOM5') for s in new_names]
	new_names 		= [s.replace('TMP','MFHL') for s in new_names]
	new_names 		= [s.replace('TP','SFHL') for s in new_names]
	new_names 		= [s.replace('TPTYP','HPO') for s in new_names]
	new_names 		= [s.replace('TMPC','HPN') for s in new_names]
	Data.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = Data.columns[ Data.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(Data[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = Data.columns[ Data.columns != VAR_FOR_RESHAPE ]
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	Data = Hilmo_DefineOutpat(Data)

	# define OPER_IN
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	Data.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data 	= Data.loc[ !Data.EVENT_AGE.isna() ]
	Data 	= Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 
	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	Data.loc[Data.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)


def Hilmo_POST95_processing(file_path:str,file_sep=';', test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# remove wrong codes
	wrong_codes = ['H','M','N','Z6','ZH','ZZ']
	Data = Data.loc[Data.PALA not in wrong_codes]

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM.str,    format='%Y-%m-%d', errors='coerce' )
	Data['DEATH_DATE'] 		= pd.to_datetime( Data.death_date.str, format='%Y-%m-%d', errors='coerce' )
	# format date columns (patient in and out dates)
	Data['TULOPVM'] 		= pd.to_datetime( Data.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	Data['LAHTOPVM']		= pd.to_datetime( Data.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )

	# define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.TULOPVM - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	Data['EVENT_YRMNTH']	= Data.TULOPVM.dt.strftime('%Y-%m')
	Data['INDEX'] 			= np.arange(Data.shape[0] ) + 1
	Data['SOURCE'] 			= 'INPAT'
	Data['CODE3']			= np.NaN
	Data['CODE4']			= (Data.LAHTOPVM - Data.TULOPVM).dt.days
	Data['ICDVER'] 			= 10

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names 

	# rename categories
	column_names 	= Data.columns
	new_names		= [s.replace('PDG','0') for s in column_names]
	new_names		= [s.replace('EDIA','EX') for s in new_names]
	new_names		= [s.replace('SDG','') for s in new_names]
	new_names 		= [s.replace('PTMPK','NOM') for s in new_names]
	new_names		= [s.replace('MTMP1K1','NOM4') for s in new_names]
	new_names		= [s.replace('MTMP2K1','NOM5') for s in new_names]
	new_names 		= [s.replace('TMP','MFHL') for s in new_names]
	new_names 		= [s.replace('TP','SFHL') for s in new_names]
	new_names 		= [s.replace('TPTYP','HPO') for s in new_names]
	new_names 		= [s.replace('TMPC','HPN') for s in new_names]
	Data.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = Data.columns[ Data.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(Data[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = Data.columns[ Data.columns != VAR_FOR_RESHAPE ]
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	Data = Hilmo_DefineOutpat(Data)

	# define OPER_IN
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	Data.loc[ Data.SOURCE=='INPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	Data.loc[ Data.SOURCE=='OUTPAT' & Data.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	Data.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data 	= Data.loc[ !Data.EVENT_AGE.isna() ]
	Data 	= Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 
	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	Data.loc[Data.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FIX OUTPAT: names and codes 
	# TODO 

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_externalreason_processing(file_path:str,file_sep=';', test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_diagnosis_processing(file_path:str,file_sep=';'):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# rename columns
	Data.rename( 
		columns = {
		'TNRO':'FINREGISTRYID',
		'SDGNRO':'CATEGORY',
		'KOODI1':'CODE1',
		'KOODI2':'CODE2',
		'TULOPVM':'PVM'},
		inplace=True )

	# define columns for detailed longitudinal
	Data['CODE4'] = np.NaN
	if Data.CODE1 in codes: 
		Data['CODE3'] = Data['CODE2']
		Data['CODE2'] = np.NaN
	else:
		Data['CODE3'] = np.NaN

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_operations_processing(file_path:str,file_sep=';', test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# rename columns
	Data.rename( columns = {
		'TOIMP':'CODE1',
		'TULOPVM':'PVM'},
		inplace=True )

	# define columns for detailed longitudinal
	Data['CATEGORY'] = Data.N + 'NOM'
	Data['CODE2']	 = np.NaN
	Data['CODE3']	 = np.NaN
	Data['CODE4']	 = np.NaN

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)


def Hilmo_heart_processing(file_path:str,file_sep=';', test=False):
	
	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# rename columns
	Data.rename( columns = {'TOIMENPIDE':'CODE1','TULOPVM':'PVM'},inplace=True )

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names

	# rename categories
	column_names 	= Data.columns
	new_names		= [s.replace('TMPTYP','HPO') for s in column_names]
	new_names 		= [s.replace('TMPC','HPN') for s in new_names]
	Data.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = Data.columns[ Data.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(Data[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TID',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = Data.columns[ Data.columns != VAR_FOR_RESHAPE ]
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TID')

	# add other (empty) code columns 
	Data['CODE2'] 	= np.NaN
	Data['CODE3'] 	= np.NaN
	Data['CODE4'] 	= np.NaN
	Data['ICDVER'] 	= 10
	Data['INDEX']  	= Hilmo.TID + '_ICD10'

	# remove missing values
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 

	#remove patient row if category is missing
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)




def AvoHilmo_icd10_processing(file_path:str,file_sep=';', test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'ICD10':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 

	# remove ICD code dots
	Data.loc[Data.CATEGORY=='ICD','CODE1'] = Data['CODE1'].replace({".", ""})

	return Data
	



def AvoHilmo_icpc2_processing(file_path:str,file_sep=';', test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'ICPC2':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 


	return Data



def AvoHilmo_oral_processing(file_path:str,file_sep=';', test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'TOIMENPIDE':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 

	return Data



def AvoHilmo_operations_processing(file_path:str,file_sep=';', test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'TOIMENPIDE':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 

	return Data



def AvoHilmo_processing(file_path:str,file_sep=';',DOB_map, extra_to_merge ,test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns ( ? )
	Data['KAYNTI_ALKOI'] 	= pd.to_datetime( Data['KAYNTI_ALKOI'].str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )
	# format date columns (birth and death date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM.str, format='%Y-%m-%d',errors='coerce' )
	Data['DEATH_DATE'] 		= pd.to_datetime( Data.death_date.str, format='%Y-%m-%d', errors='coerce')

	# check that event is after death
	Data.loc[Data.KAYNTI_ALKOI > Data.DEATH_DATE,'KAYNTI_ALKOI'] = Data.DEATH_DATE

	# define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.KAYNTI_ALKOI - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	Data['EVENT_YRMNTH']	= Data.KAYNTI_ALKOI.dt.strftime('%Y-%m')
	Data['EVENT_YEAR']		= Data.KAYNTI_ALKOI.dt.year
	Data['ICDVER']			= 10
	Data['SOURCE']			= 'PRIM_OUT'
	Data['INDEX']			= Data.AVOHILMO_ID
	Data['CODE2']			= np.NaN
	Data['CODE3']			= np.NaN
	Data['CODE4']			= np.NaN

	# rename columns
	Data.rename( 
		columns = {
		'TNRO':'FINREGISTRYID',
		'KAYNTI_YHTEYSTAPA':'CODE5',
		'KAYNTI_PALVELUMUOTO':'CODE6',
		'KAYNTI_AMMATTI':'CODE7',
		'KAYNTI_ALKOI':'PVM'},
		inplace=True )

	# merge CODE1 and CATEGORY from extra file
	Data = Data.merge(extra_to_merge, on = 'AVOHILMO_ID', how='inner')

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data = Data.loc[ !Data.EVENT_AGE.isna() ]
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)




def DeathRegistry_processing(file_path:str, file_sep=';', DOB_map, test=False):
	
	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map, on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )
	Data['dg_date']			= pd.to_datetime( Data['dg_date'], format='%Y-%m-%d' )

	# # define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.TULOPVM - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	Data['EVENT_YEAR'] 		= Data.dg_date.dt.year	
	Data['EVENT_YRMNTH']	= Data.dg_date.dt.strftime('%Y-%m')
	Data['INDEX'] 			= np.arange(Data.shape[0] ) + 1
	Data['ICDVER'] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
	Data['SOURCE'] 			= 'DEATH'
	Data['CODE2']			= np.NaN
	Data['CODE3']			= np.NaN
	Data['CODE4']			= np.NaN
	Data['CODE5']			= np.NaN
	Data['CODE6']			= np.NaN
	Data['CODE7']			= np.NaN


	# the following code will reshape the Dataframe from wide to long
	# if there was a value under one of the category columns this will be transferred under the column CODE1
	# NB: the category column values are going to be remapped after as desired  

	# perform the reshape
	CATEGORY_DICTIONARY	= {
		'TPKS':'U',
		'VKS':'I',
		'M1':'c1',
		'M2':'c2',
		'M3':'c3',
		'M4':'c4'}
	VAR_FOR_RESHAPE = CATEGORY_DICTIONARY.keys()
	TO_RESHAPE = VAR_FOR_RESHAPE.insert('TNRO')

	ReshapedData = pd.melt(Data[ TO_RESHAPE ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = Data.columns[ Data.columns != VAR_FOR_RESHAPE ]
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# rename columns
	Data.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'dg_date':'PVM'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data 	= Data.loc[ !Data.EVENT_AGE.isna() ]
	# NOT performing code check in this registry

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def CancerRegistry_processing(file_path:str, file_sep=';', DOB_map, test=False):
	
	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map, on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date and diagnosis date)
	Data['SYNTPVM'] 			= pd.to_datetime( Data.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )	
	Data['dg_date']				= pd.to_datetime( Data['dg_date'], format='%Y-%m-%d' )

	# define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.dg_date - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	Data['EVENT_YEAR'] 		= Data.dg_date.year	
	Data['EVENT_YRMNTH']	= Data.dg_date.dt.strftime('%Y-%m')
	Data['ICDVER'] 			= 'O3'
	Data['INDEX'] 			= np.arange(Data.shape[0] ) + 1
	Data['SOURCE'] 			= 'CANC'
	Data['CATEGORY'] 		= np.NaN  # maybe is 'O3' but in the code is saying to put this in ICDVER .. 
	Data['CODE4']			= np.NaN
	Data['CODE5']			= np.NaN
	Data['CODE6']			= np.NaN
	Data['CODE7']			= np.NaN

	# rename columns
	Data.rename( 
		columns = {
		'topo':'CODE1',
		'morpho':'CODE2',
		'beh':'CODE3',
		'dg_date':'PVM'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data 	= Data.loc[ !Data.EVENT_AGE.isna() ]
	Data 	= Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def KelaReimbursement_processing(file_path:str, file_sep=';', DOB_map, test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map, on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date and reimbursement date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )	
	Data['LAAKEKORVPVM']	= pd.to_datetime( Data['ALPV'], format='%Y-%m-%d' )

	# define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.LAAKEKORVPVM - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)
	Data['EVENT_YEAR'] 		= Data.LAAKEKORVPVM.year
	Data['EVENT_YRMNTH']	= Data.LAAKEKORVPVM.dt.strftime('%Y-%m')
	Data['ICDVER'] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
	Data['INDEX'] 			= np.arange(Data.shape[0]) + 1
	Data['SOURCE'] 			= 'REIMB'
	Data['CATEGORY'] 		= np.NaN
	Data['CODE3']			= np.NaN
	Data['CODE4']			= np.NaN
	Data['CODE5']			= np.NaN
	Data['CODE6']			= np.NaN
	Data['CODE7']			= np.NaN

	#rename columns
	Data.rename(
		columns = {
		'HETU':'FINREGISTRYID',
		'SK1':'CODE1',
		'DIAG':'CODE2',
		'LAAKEOSTPVM':'PVM'
		}, 
		inplace = True )

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data = Data.loc[ !Data.EVENT_AGE.isna() ]
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# remove ICD code dots
	Data['CODE2'] = Data['CODE2'].replace({".", ""})

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def KelaPurchase_processing(file_path:str, file_sep=';',DOB_map, test=False):

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'HETU',right_on = 'FINREGISTRYID')
	Data.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date and purchase date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )
	Data['LAAKEOSTPVM'] 	= pd.to_datetime( Data['OSTOPV'], format='%Y-%m-%d' ) 	

	# # define columns for detailed longitudinal
	Data['EVENT_AGE'] 		= round( (Data.LAAKEOSTPVM - Data.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)
	Data['EVENT_YEAR'] 		= Data.LAAKEOSTPVM.year
	Data['EVENT_YRMNTH']	= Data.LAAKEOSTPVM.dt.strftime('%Y-%m')
	Data['ICDVER'] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
	Data['INDEX'] 			= np.arange(Data.shape[0]) + 1
	Data['SOURCE'] 			= 'PURCH'
	Data['CATEGORY'] 		= np.NaN

	#rename columns
	Data.rename(
		columns = {
		'HETU':'FINREGISTRYID',
		'ATC':'CODE1',
		'SAIR':'CODE2',
		'VNRO':'CODE3',
		'PLKM':'CODE4',
		'KORV':'CODE5',
		'KAKORV':'CODE6',
		'LAJI':'CODE7',
		'LAAKEOSTPVM':'PVM'
		},
		inplace = True )

	# complete VNR code if shorter than 6 digits
	MISSING_DIGITS = 6 - Data.CODE3.str.len()
	ZERO = pd.Series( ['0'] * Data.shape[0] )
	Data['CODE3'] = Data.CODE3 + ZERO*MISSING_DIGITS

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.loc[Data==''] = np.NaN
	Data = Data.loc[ !Data.EVENT_AGE.isna() ]
	Data = Data.loc[ !( Data.CODE1.isna() & Data.CODE2.isna() )] 

	# FINALIZE
	Data = Data.loc[ !Data.EVENT_AGE<0 & !Data.EVENT_AGE>110]
	Data = Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	Data['CODE7'] = Data.CODE7.to_numeric
	Data = Data.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[Data.CODE7 in registry_tocheck & Data.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)

