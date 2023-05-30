
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry   
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################

# LIBRARIES

import re
import pandas as pd
import numpy as np
from datetime import datetime as dt

from config import DETAILED_LONGITUDINAL_PATH, TEST_FOLDER_PATH


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

def Write2DetailedLongitudinal(Data: pd.DataFrame, path:str = DETAILED_LONGITUDINAL_PATH):
    """Writes pandas dataframe to detailed_longitudianl

    append if already exist and also insert date in the filename


    Args:
        Data (pd.DataFrame): the dataframe to be written.
        path (str, optional): The file path to write the data to. 
                              Defaults to DETAILED_LONGITUDINAL_PATH.

    Returns:
        None

    Raises:
        ValueError: If the provided Data is not a pandas DataFrame.
        IOError: If there is an error writing the data to the specified path.
    """

	today = dt.today().strftime("%Y-%m-%d")
    filename = "detailed_longitudinal" + "_" + today + ".csv"
	Data.to_csv(path_or_buf=path+filename, mode="a", sep=';', encoding='latin-1')



def Write2TestFile(Data:pd.DataFrame, path:str = TEST_FOLDER_PATH):
	"""Writes pandas dataframe to the test file
	
	overwrite if already exist and also insert date in the filename

    Args:
        Data (pd.DataFrame): the dataframe to be written.
        path (str, optional): The file path to write the data to. 
                              Defaults to TEST_FILE_PATH.

    Returns:
        None

    Raises:
        ValueError: If the provided Data is not a pandas DataFrame.
        IOError: If there is an error writing the data to the specified path.
    """

    today = dt.today().strftime("%Y-%m-%d")
    filename = "test_detailed_longitudinal" + "_" + today + ".csv"
	Data.to_csv(path_or_buf=path+filename, mode="w", sep=';', encoding='latin-1')



def SpecialCharacterSplit(Data:pd.DataFrame):
	"""Splits the input dataframe CODE1 based on special characters.

	applies specific rules to split the CODE1 based on the presence of certain special characters.

    Args:
        Data (pd.DataFrame): the dataframe to be split.

    Returns:
        Data (pd.DataFrame): the splitted dataframe.

	Raises:
    ValueError: If the provided Data is not a pandas DataFrame.
	"""

	#------------------
	# RULE: if CODE1 has a '*' then ?!

	#------------------
	# RULE: if CODE1 has a '+' then first part goes to CODE2 and second to CODE1

	Data['IS_PLUS'] = Data.CODE1.str.match('\+')
	Data_tosplit 	= Data.loc[Data['IS_PLUS'] == True]

	if Data_tosplit.shape[0] != 0:
		Data_tosplit[['part1','part2']] = Data_tosplit['CODE1'].str.split('\+',expand=True)
		Data.loc[Data.IS_PLUS == True,'CODE2'] = Data_tosplit.part1
		Data.loc[Data.IS_PLUS == True,'CODE1'] = Data_tosplit.part2

	#------------------
	# RULE: if CODE1 has a '#' then first part goes to CODE1 and second to CODE3

	Data['IS_HAST']	= Data.CODE1.str.match('\#')
	Data_tosplit 	= Data.loc[Data['IS_HAST'] == True]

	if Data_tosplit.shape[0] != 0:
		Data_tosplit[['part1','part2']] = Data_tosplit['CODE1'].str.split('\#',expand=True)
		Data.loc[Data.IS_HAST == True,'CODE2'] = Data_tosplit.part1
		Data.loc[Data.IS_HAST == True,'CODE1'] = Data_tosplit.part2

	#------------------
	# RULE: if CODE1 has a '&' then first part goes to CODE1 and second to CODE2

	Data['IS_AND'] 	= Data.CODE1.str.match('\&')
	Data_tosplit 	= Data.loc[Data['IS_AND'] == True]

	if Data_tosplit.shape[0] != 0:
		Data_tosplit[['part1','part2']] = Data_tosplit['CODE1'].str.split('\&',expand=True)
		Data.loc[Data.IS_AND == True,'CODE2'] = Data_tosplit.part1
		Data.loc[Data.IS_AND == True,'CODE1'] = Data_tosplit.part1

	return Data	



def Hilmo_DefineOutpat(hilmo:pd.DataFrame):
	"""Define SOURCE outpat for hilmo dataframes

	applies specific rules to split the CODE1 based on the presence of certain special characters.

    Args:
        Data (pd.DataFrame): hilmo dataframe to work on.

    Returns:
        Data (pd.DataFrame): hilmo dataframe with correct SOURCEs.

	Raises:
    ValueError: If the provided Data is not a pandas DataFrame.
	"""

	# define OUTPAT after 2018
	hilmo.loc[ not ( hilmo.TULOPVM.dt.year>2018 & hilmo.YHTEYSTAPA=='R80' ),'SOURCE'] = 'OUTPAT'
	hilmo.loc[ not ( hilmo.TULOPVM.dt.year>2018 & hilmo.YHTEYSTAPA=='R10' & hilmo.PALA in [1,3,4,5,6,7,8,31] ),'SOURCE'] = 'OUTPAT'
	hilmo.loc[ not ( hilmo.TULOPVM.dt.year>2018 & hilmo.YHTEYSTAPA=='' 	& hilmo.PALA in [1,3,4,5,6,7,8,31] ),'SOURCE'] = 'OUTPAT'

	# define OUTPAT before 2018
	hilmo.loc[ hilmo.TULOPVM.dt.year<=2018 & hilmo.PALA.isna(),'SOURCE'] = 'OUTPAT'
	hilmo.loc[ hilmo.TULOPVM.dt.year<=2018 & hilmo.PALA not in [1,3,4,5,6,7,8,31],'SOURCE'] = 'OUTPAT'

	# NB: all years before 1998 are INPAT (QC)
	hilmo.loc[ hilmo.TULOPVM.dt.year<1998,'SOURCE'] = 'INPAT'

	return hilmo


##########################################################
# REGISTRY-SPECIFIC FUNCTIONS

def Hilmo_69_86_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the Hilmo information from 1969 to 1986.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( columns ={'date_of_birth':'SYNTPVM'}, inplace = True )

	# format date columns (birth and death date)
	Data['SYNTPVM'] 		= pd.to_datetime( Data.SYNTPVM, format='%Y-%m-%d',errors='coerce' )
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
	Data['CODE5']			= np.NaN
	Data['CODE6']			= np.NaN
	Data['CODE7']			= np.NaN


	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names

	# perform the reshape
	CATEGORY_DICTIONARY = {"DG1": "0", "DG2": "1", "DG3": "2", "DG4": "3"}
	reshaped_data = pd.melt(Data, 
		id_vars=["TNRO"], 
		value_vars=CATEGORY_DICTIONARY.keys(), 
		var_name="CATEGORY", 
		value_name="CODE1")
	reshaped_data["CATEGORY"].replace(CATEGORY_DICTIONARY, inplace=True)
	Data = reshaped_data.merge( Data.drop(CATEGORY_DICTIONARY.keys(), axis=1), on="TNRO")

	# define OUTPAT
	# not in this hilmo

	# define OPER_IN
	# not in this hilmo

	# define OPER_OUT
	# not in this hilmo

	#rename columns
	Data.rename( columns = {'TULOPVM':'PVM',}, inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN

	# check special characters
	Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital'

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_87_93_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the Hilmo information from 1987 to 1993.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )

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
	CATEGORY_DICTIONARY = {
	# FULL RENAME
	'PDG':'0',
	'EDIA':'EX',
	'MTMP1K1':'NOM4',
	'MTMP2K1':'NOM5',
	# PREFIX RENAME
	'SDG':'',
	'PTMPK':'NOM',
	'TMP':'MFHL',
	'TP':'SFHL',
	'TPTYP':'HPO',
	'TMPC':'HPN'
	 }

	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in column_names]

	# perform the reshape
	VAR_FOR_RESHAPE = set(new_names)^set(column_names)
	TO_RESHAPE = VAR_FOR_RESHAPE + ['TNRO']

	ReshapedData = pd.melt(Data[ TO_RESHAPE ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = set(Data.columns)^set(VAR_FOR_RESHAPE)
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
		'TULOPVM':'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN

	# check special characters
	Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_94_95_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the Hilmo information from 1994 to 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )

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
	CATEGORY_DICTIONARY = {
	# FULL RENAME
	'PDG':'0',
	'EDIA':'EX',
	'MTMP1K1':'NOM4',
	'MTMP2K1':'NOM5',
	# PREFIX RENAME
	'SDG':'',
	'PTMPK':'NOM',
	'TMP':'MFHL',
	'TP':'SFHL',
	'TPTYP':'HPO',
	'TMPC':'HPN'
	 }

	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in column_names]

	# perform the reshape
	VAR_FOR_RESHAPE = set(new_names)^set(column_names)
	TO_RESHAPE = VAR_FOR_RESHAPE + ['TNRO']

	ReshapedData = pd.melt(Data[ TO_RESHAPE ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = set(Data.columns)^set(VAR_FOR_RESHAPE)
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
		'TULOPVM':'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN

	# check special characters
	Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital'	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)


def Hilmo_POST95_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the Hilmo information after 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# remove wrong codes
	wrong_codes = ['H','M','N','Z6','ZH','ZZ']
	Data = Data.loc[Data.PALA not in wrong_codes].reset_index(drop=True,inplace=True)

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )

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
	CATEGORY_DICTIONARY = {
	# FULL RENAME
	'PDG':'0',
	'EDIA':'EX',
	'MTMP1K1':'NOM4',
	'MTMP2K1':'NOM5',
	# PREFIX RENAME
	'SDG':'',
	'PTMPK':'NOM',
	'TMP':'MFHL',
	'TP':'SFHL',
	'TPTYP':'HPO',
	'TMPC':'HPN'
	 }

	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in column_names]

	# perform the reshape
	VAR_FOR_RESHAPE = set(new_names)^set(column_names)
	TO_RESHAPE = VAR_FOR_RESHAPE + ['TNRO']

	ReshapedData = pd.melt(Data[ TO_RESHAPE ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = set(Data.columns)^set(VAR_FOR_RESHAPE)
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
		'TULOPVM':'PVM',
		'PALA':'CODE5',
		'EA':'CODE6',
		'PALTU':'CODE7'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,'CODE4'] = np.NaN

	# check special characters
	Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),'CODE1'] = np.NaN

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# special character split
	Data = SpecialCharacterSplit(Data)

	# FIX OUTPAT: names and codes 
	# TODO 

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital'

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_externalreason_processing(file_path:str,file_sep=';', test=False):
	"""Process Hilmo external reason of death.

    This function reads and processes an Hilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def Hilmo_diagnosis_processing(file_path:str,file_sep=';', test=False):
	"""Process Hilmo diagnosis.

    This function reads and processes an Hilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

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
	"""Process Hilmo surgical operations.

    This function reads and processes an Hilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# rename columns
	Data.rename( 
		columns = {
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
	"""Process Hilmo heart surgeries.

    This function reads and processes an Hilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """
	
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
	TO_RESHAPE = VAR_FOR_RESHAPE + ['TNRO']

	ReshapedData = pd.melt(Data[ TO_RESHAPE ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = set(Data.columns)^set(VAR_FOR_RESHAPE)
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# add other (empty) code columns 
	Data['CODE2'] 	= np.NaN
	Data['CODE3'] 	= np.NaN
	Data['CODE4'] 	= np.NaN
	Data['ICDVER'] 	= 10
	Data['INDEX']  	= Hilmo.TID + '_ICD10'

	# remove missing values
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	#remove patient row if category is missing
	Data = SpecialCharacterSplit(Data)

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital'	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)




def AvoHilmo_icd10_processing(file_path:str,file_sep=';', test=False):
	"""Process AvoHilmo ICD10 diagnosis.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'ICD10':'CODE1'}, inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ Data.CODE1.notna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ not( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 

	# remove ICD code dots
	Data.loc[Data.CATEGORY=='ICD','CODE1'] = Data['CODE1'].replace({".", ""})

	return Data
	



def AvoHilmo_icpc2_processing(file_path:str,file_sep=';', test=False):
	"""Process AvoHilmo ICPC2 diagnosis.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'ICPC2':'CODE1'}, inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ Data.CODE1.notna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ not( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 


	return Data



def AvoHilmo_oral_processing(file_path:str,file_sep=';', test=False):
	"""Process AvoHilmo oral operation.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'TOIMENPIDE':'CODE1'}, inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ Data.CODE1.notna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ not( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 

	return Data



def AvoHilmo_operations_processing(file_path:str,file_sep=';', test=False):
	"""Process AvoHilmo surgery operations.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')
	Data.rename( columns = {'TOIMENPIDE':'CODE1'},inplace=True )

	# define the category column 
	Data['CATEGORY'] = np.NaN
	to_update = Data.iloc[ Data.CODE1.notna() ]
	Data[to_update,'CATEGORY'] = Data[to_update,'CODE1'] + Data[to_update,'JARJESTYS']

	# filter data
	Data = Data.loc[ not( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 

	return Data



def AvoHilmo_processing(file_path:str, DOB_map, extra_to_merge, file_sep=';', test=False):
	"""Process AvoHilmo general file.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    also reads an extra AvoHilmo dataset about diagnosis/operations in order to add CODE1 and CATEGORY columns. 
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        extra_to_merge (pd.dataframe, optional): dataframe to map CODE1 and CATEGORY inside the AvoHilmo file
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
        ValueError: If the provided extra_to_merge is not a pandas DataFrame.
    """

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )

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
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital'	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)




def DeathRegistry_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the information from death registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map, on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )
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
	VAR_FOR_RESHAPE = list(CATEGORY_DICTIONARY.keys())
	TO_RESHAPE = VAR_FOR_RESHAPE + ['TNRO']

	ReshapedData = pd.melt(Data[ TO_RESHAPE ],
		id_vars 	= 'TNRO',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')
	ReshapedData['CATEGORY'].replace(CATEGORY_DICTIONARY,inplace=True)

	#create the final Dataset
	VAR_NOT_FOR_RESHAPE = set(Data.columns)^set(VAR_FOR_RESHAPE)
	Data = ReshapedData.merge(Data[ VAR_NOT_FOR_RESHAPE ], on = 'TNRO')

	# rename columns
	Data.rename( 
		columns = {
		'dg_date':'PVM'
		},
		inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	# NOT performing code check in this registry

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def CancerRegistry_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the information from cancer registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map, on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )
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
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital' 

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def KelaReimbursement_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the information from kela reimbursement registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	
	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map, on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )
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
		'SK1':'CODE1',
		'DIAG':'CODE2',
		'LAAKEOSTPVM':'PVM'
		}, 
		inplace = True )

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# remove missing values
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# remove duplicates
	Data.drop_duplicates(keep='first', inplace=True)

	# remove ICD code dots
	Data['CODE2'] = Data['CODE2'].replace({".", ""})

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110)].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital' 	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)



def KelaPurchase_processing(file_path:str, DOB_map, file_sep=';', test=False):
	"""Process the information from kela purchases registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ';'.
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	
	# fetch Data
	if test: 	Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding='latin-1')		
	else: 		Data = pd.read_csv(file_path, sep = file_sep, encoding='latin-1')

	# add date of birth
	Data = Data.merge(DOB_map,left_on = 'HETU',right_on = 'FINREGISTRYID')
	Data.rename( columns = {'date_of_birth':'SYNTPVM'}, inplace = True )
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
	Data.mask(Data == '' ,inplace=True)
	Data.loc[ Data.EVENT_AGE.notna() ,].reset_index(drop=True,inplace=True)
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 

	# FINALIZE
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	Data.sort_values( by = ['FINREGISTRYID','EVENT_AGE'], inplace=True)

	# add PALTU info
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=',')
	Data['CODE7'] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	Data.loc[ Data.CODE7.isin(registry_tocheck) & Data.CODE7.isna(),'CODE7'] = 'Other Hospital'

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)

