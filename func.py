
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry   
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################

# LIBRARIES

import re
import pandas as pd
import numpy as np

from config import DETAILED_LONGITUDINAL_PATH


#-------------------
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

#--------------------
# UTILITY FUNCTIONS

def Write2DetailedLongitudinal(df, path=DETAILED_LONGITUDINAL_PATH):
	df.to_csv(mode="a")


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


#---------------------
# REGISTRY-SPECIFIC FUNCTIONS

def Hilmo_69_86_preparation(file_path:str, file_sep=';',DOB_map):

	# fetch Data
	Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	NewData = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM,    format='%Y-%m-%d',errors='coerce' )
	NewData['DEATH_DATE'] 	= pd.to_datetime( NewData.death_date, format='%Y-%m-%d',errors='coerce' )
	# format date columns (patient in and out dates)
	NewData['TULOPVM'] 		= pd.to_datetime( NewData.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	NewData['LAHTOPVM']		= pd.to_datetime( NewData.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )

	# check that event is after death
	NewData.loc[NewData.TULOPVM > NewData.DEATH_DATE,'TULOPVM'] = NewData.DEATH_DATE

	# define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.TULOPVM - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YRMNTH']	= NewData.TULOPVM.dt.strftime('%Y-%m')
	NewData['INDEX'] 		= np.arange(NewData.shape[0]) + 1
	NewData['SOURCE'] 		= 'INPAT'
	NewData['ICDVER'] 		= 8
	NewData['CODE2']		= np.NaN
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= (NewData.LAHTOPVM - NewData.TULOPVM).dt.days

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

	ReshapedData = pd.melt(NewData[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final dataset
	VAR_NOT_FOR_RESHAPE = NewData.columns[ NewData.columns != VAR_FOR_RESHAPE ]
	ReshapedFinal = ReshapedData.merge(NewData[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	FinalData = Hilmo_DefineOutpat(ReshapedFinal)

	# define OPER_IN
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	FinalData.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	SubsetData = FinalData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 
	# if negative hospital days than missing value
	CodeCheck.loc[CodeCheck.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	CodeCheck.loc[CodeCheck.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates ?
	...

	# special character split
	...

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def Hilmo_87_93_preparation(file_path:str, file_sep=';',DOB_map):

	# fetch Data
	Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	NewData = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM.str,    format='%Y-%m-%d', errors='coerce' )
	NewData['DEATH_DATE'] 	= pd.to_datetime( NewData.death_date.str, format='%Y-%m-%d', errors='coerce')
	# format date columns (patient in and out dates)
	NewData['TULOPVM'] 		= pd.to_datetime( NewData.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	NewData['LAHTOPVM']		= pd.to_datetime( NewData.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )

	# check that event is after death
	NewData.loc[NewData.TULOPVM > NewData.DEATH_DATE,'TULOPVM'] = NewData.DEATH_DATE

	# define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.TULOPVM - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YRMNTH']	= NewData.TULOPVM.dt.strftime('%Y-%m')
	NewData['INDEX'] 		= np.arange(NewData.shape[0]) + 1
	NewData['SOURCE'] 		= 'INPAT'
	NewData['ICDVER'] 		= 9
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= (NewData.LAHTOPVM - NewData.TULOPVM).dt.days

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names  

	# rename categories 
	column_names 	= NewData.columns
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
	NewData.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = NewData.columns[ NewData.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(NewData[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = NewData.columns[ NewData.columns != VAR_FOR_RESHAPE ]
	ReshapedFinal = ReshapedData.merge(NewData[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	FinalData = Hilmo_DefineOutpat(ReshapedFinal)

	# define OPER_IN
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	FinalData.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	SubsetData = FinalData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 
	# if negative hospital days than missing value
	CodeCheck.loc[CodeCheck.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	CodeCheck.loc[CodeCheck.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates ?
	...

	# special character split
	...


	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def Hilmo_94_95_preparation(file_path:str, file_sep=';',DOB_map):

	# fetch Data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# add date of birth
	NewData = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM.str,    format='%Y-%m-%d', errors='coerce' )
	NewData['DEATH_DATE'] 	= pd.to_datetime( NewData.death_date.str, format='%Y-%m-%d', errors='coerce' )
	# format date columns (patient in and out dates)
	NewData['TULOPVM'] 		= pd.to_datetime( NewData.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	NewData['LAHTOPVM']		= pd.to_datetime( NewData.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )
	
	# check that event is after death
	NewData.loc[NewData.TULOPVM > NewData.DEATH_DATE,'TULOPVM'] = NewData.DEATH_DATE

	# define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.TULOPVM - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YRMNTH']	= NewData.TULOPVM.dt.strftime('%Y-%m')
	NewData['INDEX'] 		= np.arange(NewData.shape[0] ) + 1
	NewData['SOURCE'] 		= 'INPAT'
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= (NewData.LAHTOPVM - NewData.TULOPVM).dt.days
	NewData['ICDVER'] 		= 9

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names  

	# rename categories
	column_names 	= NewData.columns
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
	NewData.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = NewData.columns[ NewData.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(NewData[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = NewData.columns[ NewData.columns != VAR_FOR_RESHAPE ]
	ReshapedFinal = ReshapedData.merge(NewData[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	FinalData = Hilmo_DefineOutpat(ReshapedFinal)

	# define OPER_IN
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	FinalData.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	SubsetData = FinalData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 
	# if negative hospital days than missing value
	CodeCheck.loc[CodeCheck.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	CodeCheck.loc[CodeCheck.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates ?
	...

	# special character split
	...


	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def Hilmo_POST95_Preparation(file_path:str,file_sep=';'):

	# fetch Data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# remove wrong codes
	wrong_codes = ['H','M','N','Z6','ZH','ZZ']
	FilteredData = Data.loc[Data.PALA not in wrong_codes]

	# add date of birth
	NewData = FilteredData.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns (birth and death date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM.str,    format='%Y-%m-%d', errors='coerce' )
	NewData['DEATH_DATE'] 	= pd.to_datetime( NewData.death_date.str, format='%Y-%m-%d', errors='coerce' )
	# format date columns (patient in and out dates)
	NewData['TULOPVM'] 		= pd.to_datetime( NewData.TULOPV.str.slice(stop=10),  format='%d.%m.%Y',errors='coerce' )
	NewData['LAHTOPVM']		= pd.to_datetime( NewData.LAHTOPV.str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )

	# define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.TULOPVM - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YRMNTH']	= NewData.TULOPVM.dt.strftime('%Y-%m')
	NewData['INDEX'] 		= np.arange(NewData.shape[0] ) + 1
	NewData['SOURCE'] 		= 'INPAT'
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= (NewData.LAHTOPVM - NewData.TULOPVM).dt.days
	NewData['ICDVER'] 		= 10

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names 

	# rename categories
	column_names 	= NewData.columns
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
	NewData.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = NewData.columns[ NewData.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(NewData[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = NewData.columns[ NewData.columns != VAR_FOR_RESHAPE ]
	ReshapedFinal = ReshapedData.merge(NewData[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# define OUTPAT
	FinalData = Hilmo_DefineOutpat(ReshapedFinal)

	# define OPER_IN
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_IN'
	FinalData.loc[ FinalData.SOURCE=='INPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_IN'

	# define OPER_OUT
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('NOM'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPN'),'SOURCE'] = 'OPER_OUT'
	FinalData.loc[ FinalData.SOURCE=='OUTPAT' & FinalData.CATEGORY.str.contains('HPO'),'SOURCE'] = 'OPER_OUT'

	#rename columns
	FinalData.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'TULOPVM' :'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	SubsetData = FinalData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 
	# if negative hospital days than missing value
	CodeCheck.loc[CodeCheck.CODE4<0,'CODE4'] = np.NaN
	# check special characters
	CodeCheck.loc[CodeCheck.CODE1 in ["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"],'CODE1'] = np.NaN

	# remove duplicates ?	
	...

	# special character split
	...	

	# FIX OUTPAT: names and codes 
	...

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 


	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def Hilmo_externalreason_preparation(file_path:str,file_sep=';'):

	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	return ...

	

def Hilmo_diagnosis_preparation(file_path:str,file_sep=';'):

	#fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

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
	Write2DetailedLongitudinal(CodeCheck)



def Hilmo_operations_preparation(file_path:str,file_sep=';'):

	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')	

	# rename columns
	Data.rename( columns = {
		'TOIMP':'CODE1',
		'TULOPVM':'PVM'},
		inplace=True )

	# define columns for detailed longitudinal
	Data['CATEGORY'] = Data.N + 'NOM'
	Data['CODE2'] = np.NaN
	Data['CODE3'] = np.NaN
	Data['CODE4'] = np.NaN

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)


def Hilmo_heart_preparation(file_path:str,file_sep=';'):
	
	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# rename columns
	Data.rename( columns = {'TOIMENPIDE':'CODE1','TULOPVM':'PVM'},inplace=True )

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names

	# rename categories
	column_names 	= MergedData.columns
	new_names		= [s.replace('TMPTYP','HPO') for s in column_names]
	new_names 		= [s.replace('TMPC','HPN') for s in new_names]
	MergedData.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = MergedData.columns[ MergedData.columns in [set(new_names)^set(column_names)] ]

	ReshapedData = pd.melt(NewData[:, VAR_FOR_RESHAPE.insert('TNRO') ],
		id_vars 	= 'TID',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = MergedData.columns[ MergedData.columns != VAR_FOR_RESHAPE ]
	FinalData = ReshapedData.merge(MergedData[ VAR_NOT_FOR_RESHAPE ], on = 'TID')

	# add other (empty) code columns 
	FinalData['CODE2'] 	= np.NaN
	FinalData['CODE3'] 	= np.NaN
	FinalData['CODE4'] 	= np.NaN
	FinalData['ICDVER'] = 10
	FinalData['INDEX']  = Hilmo.TID + '_ICD10'

	# remove missing values
	AgeCheck = FinalData.loc[ !( FinalData.CODE1.isna() & FinalData.CODE2.isna() )] 

	#remove patient row if category is missing
	...

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)




def AvoHilmo_icd10_preparation(file_path:str,file_sep=';'):

	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'ICD10':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	FinalData = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 

	# remove ICD code dots
	FinalData.loc[FinalData.CATEGORY=='ICD','CODE1'] = FinalData['CODE1'].replace({".", ""})

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)
	



def AvoHilmo_icpc2_preparation(file_path:str,file_sep=';'):

	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'ICPC2':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	FinalData = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 


	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def AvoHilmo_oral_preparation(file_path:str,file_sep=';'):

	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'TOIMENPIDE':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	FinalData = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def AvoHilmo_operations_preparation(file_path:str,file_sep=';'):

	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'TOIMENPIDE':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ !Data.CODE1.isna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	FinalData = Data.loc[ !( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )] 

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def AvoHilmo_preparation(file_path:str,file_sep=';',DOB_map):

	# fetch data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# add date of birth
	NewData = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )

	# format date columns ( ? )
	NewData['KAYNTI_ALKOI'] = pd.to_datetime( NewData['KAYNTI_ALKOI'].str.slice(stop=10), format='%d.%m.%Y',errors='coerce' )
	# format date columns (birth and death date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM.str, format='%Y-%m-%d',errors='coerce' )
	NewData['DEATH_DATE'] 	= pd.to_datetime( NewData.death_date.str, format='%Y-%m-%d', errors='coerce')

	# check that event is after death
	NewData.loc[NewData.KAYNTI_ALKOI > NewData.DEATH_DATE,'KAYNTI_ALKOI'] = NewData.DEATH_DATE

	# define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.KAYNTI_ALKOI - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YRMNTH']	= NewData.KAYNTI_ALKOI.dt.strftime('%Y-%m')
	NewData['EVENT_YEAR']	= NewData.KAYNTI_ALKOI.dt.year
	NewData['ICDVER']		= 10
	NewData['SOURCE']		= 'PRIM_OUT'
	NewData['INDEX']		= NewData.AVOHILMO_ID
	NewData['CODE2']		= np.NaN
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= np.NaN

	# rename columns
	NewData.rename( 
		columns = {
		'TNRO':'FINREGISTRYID',
		'KAYNTI_YHTEYSTAPA':'CODE5',
		'KAYNTI_PALVELUMUOTO':'CODE6',
		'KAYNTI_AMMATTI':'CODE7',
		'KAYNTI_ALKOI':'PVM'},
		inplace=True )

	# select desired columns 
	SubsetData = FinalData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# remove duplicates ?
	...

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)




def DeathRegistry_preparation(file_path:str, file_sep=';', DOB_map):
	
	# fetch Data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = Data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )
	NewData['dg_date']		= pd.to_datetime( NewData['dg_date'], format='%Y-%m-%d' )

	# # define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.TULOPVM - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YEAR'] 	= NewData.dg_date.dt.year	
	NewData['EVENT_YRMNTH']	= NewData.dg_date.dt.strftime('%Y-%m')
	NewData['INDEX'] 		= np.arange(NewData.shape[0] ) + 1
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	NewData['SOURCE'] 		= 'DEATH'
	NewData['CODE2']		= np.NaN
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= np.NaN
	NewData['CODE5']		= np.NaN
	NewData['CODE6']		= np.NaN
	NewData['CODE7']		= np.NaN


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

	ReshapedData = pd.melt(NewData[ TO_RESHAPE ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = NewData.columns[ NewData.columns != VAR_FOR_RESHAPE ]
	FinalData = ReshapedData.merge(NewData[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# rename columns
	FinalData.rename( 
		columns = {
		'TNRO':'FINREGISTRYID'
		'dg_date':'PVM'
		},
		inplace=True)

	# select desired columns 
	SubsetData = FinalData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	# NOT performing code check in this registry

	# remove duplicates ?
	...

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def CancerRegistry_preparation(file_path:str, file_sep=';', DOB_map):
	
	# fetch Data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = Data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date and diagnosis date)
	NewData['SYNTPVM'] 			= pd.to_datetime( NewData.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )	
	NewData['dg_date']			= pd.to_datetime( NewData['dg_date'], format='%Y-%m-%d' )

	# # define columns for detailed longitudinal
	NewData['EVENT_AGE'] 		= round( (NewData.dg_date - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YEAR'] 		= NewData.dg_date.year	
	NewData['EVENT_YRMNTH']		= NewData.dg_date.dt.strftime('%Y-%m')
	NewData['ICDVER'] 			= 'O3'
	NewData['INDEX'] 			= np.arange(NewData.shape[0] ) + 1
	NewData['SOURCE'] 			= 'CANC'
	NewData['CATEGORY'] 		= np.NaN  # maybe is 'O3' but in the code is saying to put this in ICDVER .. 
	NewData['CODE4']			= np.NaN
	NewData['CODE5']			= np.NaN
	NewData['CODE6']			= np.NaN
	NewData['CODE7']			= np.NaN

	# rename columns
	NewData.rename( 
		columns = {
		'topo':'CODE1',
		'morpho':'CODE2',
		'beh':'CODE3',
		'dg_date':'PVM'
		},
		inplace=True)

	# select desired columns 
	SubsetData = NewData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# remove duplicates ?
	...

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def KelaReimbursement_preparation(file_path:str, file_sep=';', DOB_map):

	# fetch Data
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = Data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date and reimbursement date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )	
	NewData['LAAKEKORVPVM']	= pd.to_datetime( NewData['ALPV'], format='%Y-%m-%d' )

	# define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.LAAKEKORVPVM - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)
	NewData['EVENT_YEAR'] 	= NewData.LAAKEKORVPVM.year
	NewData['EVENT_YRMNTH']	= NewData.LAAKEKORVPVM.dt.strftime('%Y-%m')
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	NewData['INDEX'] 		= np.arange(NewData.shape[0]) + 1
	NewData['SOURCE'] 		= 'REIMB'
	NewData['CATEGORY'] 	= np.NaN
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= np.NaN
	NewData['CODE5']		= np.NaN
	NewData['CODE6']		= np.NaN
	NewData['CODE7']		= np.NaN

	#rename columns
	NewData.rename(
		columns = {
		'HETU':'FINREGISTRYID',
		'SK1':'CODE1',
		'DIAG':'CODE2',
		'LAAKEOSTPVM':'PVM'
		}, 
		inplace = True )

	# select desired columns 
	SubsetData = NewData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# remove duplicates ?
	...

	# remove ICD code dots
	CodeCheck['CODE2'] = CodeCheck['CODE2'].replace({".", ""})

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)



def KelaPurchase_preparation(file_path:str, file_sep=';',DOB_map):

	# create aggregated Dataset
	Data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = Data.merge(DOB_map,left_on = 'HETU',right_on = 'FINREGISTRYID')
	NewData.rename( 'date_of_birth':'SYNTPVM', inplace = True )
	# format date columns (birth date and purchase date)
	NewData['SYNTPVM'] 		= pd.to_datetime( NewData.SYNTPVM.str.slice(stop=10), format='%d-%m-%Y' )
	NewData['LAAKEOSTPVM'] 	= pd.to_datetime( NewData['OSTOPV'], format='%Y-%m-%d' ) 	

	# # define columns for detailed longitudinal
	NewData['EVENT_AGE'] 	= round( (NewData.LAAKEOSTPVM - NewData.SYNTPVM).dt.days/DAYS_TO_YEARS, 2)
	NewData['EVENT_YEAR'] 	= NewData.LAAKEOSTPVM.year
	NewData['EVENT_YRMNTH']	= NewData.LAAKEOSTPVM.dt.strftime('%Y-%m')
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	NewData['INDEX'] 		= np.arange(NewData.shape[0]) + 1
	NewData['SOURCE'] 		= 'PURCH'
	NewData['CATEGORY'] 	= np.NaN

	#rename columns
	NewData.rename(
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
	MISSING_DIGITS = 6 - NewData.CODE3.str.len()
	ZERO = pd.Series( ['0'] * NewData.shape[0] )
	NewData['CODE3'] = NewData.CODE3 + ZERO*MISSING_DIGITS

	# select desired columns 
	SubsetData = NewData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	Write2DetailedLongitudinal(CodeCheck)

