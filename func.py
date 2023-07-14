
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry  2023
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################

# LIBRARIES

import re
import pandas as pd
import numpy as np
from datetime import datetime as dt
from pathlib import Path

from config import DETAILED_LONGITUDINAL_PATH, TEST_FOLDER_PATH


##########################################################
# UTILITY VARIABLES

DAYS_TO_YEARS = 365.24

PALA_INPAT_LIST = [1,3,4,5,6,7,8,31]

COLUMNS_2_KEEP = [
	"FINREGISTRYID",
	"PVM", 
	"EVENT_YRMNTH", 
	"EVENT_AGE", 
	"INDEX",
	"SOURCE",
	"ICDVER",
	"CATEGORY",
	"CODE1", 
	"CODE2", 
	"CODE3", 
	"CODE4", 
	"CODE5",
	"CODE6", 
	"CODE7"]



##########################################################
# UTILITY FUNCTIONS

def Write2DetailedLongitudinal(Data: pd.DataFrame, path = DETAILED_LONGITUDINAL_PATH ,header = False):
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

    filename = "detailed_longitudinal.csv"
    #remove header if file is already existing
	Data.to_csv(
		path_or_buf= Path(path)/filename, 
		mode="a", 
		sep=",", 
		encoding="latin-1", 
		index=False,
		header=header)



def Write2TestFile(Data:pd.DataFrame, path = TEST_FOLDER_PATH, header = False):
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

    today = dt.today().strftime("%Y_%m_%d")
    filename = "test_detailed_longitudinal" + "_" + today + ".csv"
    #remove header if file is already existing
	Data.to_csv(
		path_or_buf= Path(path)/filename, 
		mode="a", 
		sep=",", 
		encoding="latin-1", 
		index=False,
		header=header)


def CombinationCodesSplit(Data:pd.DataFrame):
	"""Splits the input dataframe CODE1 based on special characters.

	applies specific rules to split the CODE1 based on the presence of combination codes.
	NB: check out FinnGen research handbook for more info

    Args:
        Data (pd.DataFrame): the dataframe to be split.

    Returns:
        Data (pd.DataFrame): the splitted dataframe.

	Raises:
    ValueError: If the provided Data is not a pandas DataFrame.
	"""

	Data["IS_STAR"] = Data.CODE1.str.contains("\*")
	Data_tosplit 	= Data.loc[Data["IS_STAR"] == True]

	if Data_tosplit.shape[0] != 0:
		Data_tosplit[["part1","part2"]] = Data_tosplit["CODE1"].str.split(pat = "\*",expand=True)[[0,1]]
		Data.loc[Data.IS_STAR == True,"CODE1"] = Data_tosplit.part1
		Data.loc[Data.IS_STAR == True,"CODE2"] = Data_tosplit.part2

	#------------------
	Data["IS_AND"] 	= Data.CODE1.str.contains("\&")
	Data_tosplit 	= Data.loc[Data["IS_AND"] == True]

	if Data_tosplit.shape[0] != 0:
		Data_tosplit[["part1","part2"]] = Data_tosplit["CODE1"].str.split(pat = "\&",expand=True)[[0,1]]
		Data.loc[Data.IS_AND == True,"CODE1"] = Data_tosplit.part1
		Data.loc[Data.IS_AND == True,"CODE2"] = Data_tosplit.part1

	#------------------
	Data["IS_HAST"]	= Data.CODE1.str.contains("\#")
	Data_tosplit 	= Data.loc[Data["IS_HAST"] == True]

	if Data_tosplit.shape[0] != 0:
		Data_tosplit[["part1","part2"]] = Data_tosplit["CODE1"].str.split(pat = "\#",expand=True)[[0,1]]
		Data.loc[Data.IS_HAST == True,"CODE1"] = Data_tosplit.part1
		Data.loc[Data.IS_HAST == True,"CODE3"] = Data_tosplit.part2

	#------------------
	Data["IS_PLUS"] = Data.CODE1.str.contains("\+")
	Data_tosplit 	= Data.loc[Data["IS_PLUS"] == True]

	if Data_tosplit.shape[0] != 0:
		Data_tosplit[["part1","part2"]] = Data_tosplit["CODE1"].str.split(pat = "\+",expand=True)[[0,1]]
		Data.loc[Data.IS_PLUS == True,"CODE2"] = Data_tosplit.part1
		Data.loc[Data.IS_PLUS == True,"CODE1"] = Data_tosplit.part2

	return Data	



def Define_INPAT(Data:pd.DataFrame):
	"""Define SOURCE outpat for hilmo dataframes

	applies specific rules to define if the hilmo source needs to be defined as INPAT

    Args:
        Data (pd.DataFrame): hilmo dataframe to work on.

    Returns:
        Data (pd.DataFrame): hilmo dataframe with correct SOURCEs.

	Raises:
    ValueError: If the provided Data is not a pandas DataFrame.
	"""

	# RULE 1969-1997: all is INPAT
	YEAR_TO_KEEP = (Data.PVM.dt.year<1998)
	Data.loc[ YEAR_TO_KEEP ,"SOURCE"] = "INPAT"

	# RULE 1998-2018: OUTPAT depends on PALA variable
	YEAR_TO_KEEP = (Data.PVM.dt.year>=1998) & (Data.PVM.dt.year<=2018)
	Data.loc[ YEAR_TO_KEEP & Data.PALA.isna(),"SOURCE"] = "INPAT"
	Data.loc[ YEAR_TO_KEEP & Data.PALA.isin(PALA_INPAT_LIST),"SOURCE"] = "INPAT"

	# RULE 2019-NOW: OUTPAT depends on PALA and YHTEYSTAPA variable
	YEAR_TO_KEEP = (Data.PVM.dt.year>2018)
	Data.loc[ YEAR_TO_KEEP & (Data.YHTEYSTAPA=="R80"), "SOURCE"] = "INPAT"
	Data.loc[ YEAR_TO_KEEP & (Data.YHTEYSTAPA=="R10") & (Data.PALA.isin(PALA_INPAT_LIST)), "SOURCE"] = "INPAT"
	Data.loc[ YEAR_TO_KEEP & (Data.YHTEYSTAPA==""   ) & (Data.PALA.isin(PALA_INPAT_LIST)), "SOURCE"] = "INPAT"

	return Data


def Define_OPERIN(Data:pd.DataFrame):
	"""Define SOURCE outpat for hilmo dataframes

	applies specific rules to define if the hilmo source needs to be defined as OPER_IN

    Args:
        Data (pd.DataFrame): hilmo dataframe to work on.

    Returns:
        Data (pd.DataFrame): hilmo dataframe with correct SOURCEs.

	Raises:
    ValueError: If the provided Data is not a pandas DataFrame.
	"""

	Data.loc[ (Data.SOURCE=="INPAT") & (Data.CATEGORY.str.contains("NOM")), "SOURCE"] = "OPER_IN"
	Data.loc[ (Data.SOURCE=="INPAT") & (Data.CATEGORY.str.contains("HPN")), "SOURCE"] = "OPER_IN"
	Data.loc[ (Data.SOURCE=="INPAT") & (Data.CATEGORY.str.contains("HPO")), "SOURCE"] = "OPER_IN"

	return Data


def Define_OPEROUT(Data:pd.DataFrame):
	"""Define SOURCE outpat for hilmo dataframes

	applies specific rules to define if the hilmo source needs to be defined as OPER_OUT

    Args:
        Data (pd.DataFrame): hilmo dataframe to work on.

    Returns:
        Data (pd.DataFrame): hilmo dataframe with correct SOURCEs.

	Raises:
    ValueError: If the provided Data is not a pandas DataFrame.
	"""

	Data.loc[ (Data.SOURCE=="OUTPAT") & (Data.CATEGORY.str.contains("NOM")), "SOURCE"] = "OPER_OUT"
	Data.loc[ (Data.SOURCE=="OUTPAT") & (Data.CATEGORY.str.contains("HPN")), "SOURCE"] = "OPER_OUT"
	Data.loc[ (Data.SOURCE=="OUTPAT") & (Data.CATEGORY.str.contains("HPO")), "SOURCE"] = "OPER_OUT"

	return Data


##########################################################
# REGISTRY-SPECIFIC FUNCTIONS

def Hilmo_69_86_processing(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process the Hilmo information from 1969 to 1986.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
        file_sep (str, optional): The separator used in the file. Defaults to ";".    
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """

    dtypes = {
    "TNRO": str,
    "TULOPV":  str, 
    "LAHTOPV": str,
    "DG1": str,
	"DG2": str,
	"DG3": str,
	"DG4": str,
	"TP1": str,
	"TP2": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# add date of birth
	Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
	Data.rename( columns ={"date_of_birth":"BIRTH_DATE"}, inplace = True )

	# format date columns (birth and death date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d",errors="coerce" )
	Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d",errors="coerce" )
	# format date columns (patient in and out dates)
	Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TULOPV.str.slice(stop=10),  format="%d.%m.%Y",errors="coerce" )
	Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LAHTOPV.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

	# check if event is after death
	Data.loc[Data.ADMISSION_DATE > Data.DEATH_DATE,"ADMISSION_DATE"] = Data.DEATH_DATE

	#-------------------------------------------
	# define columns for detailed longitudinal

	Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
	Data["EVENT_YRMNTH"]	= Data.ADMISSION_DATE.dt.strftime("%Y-%m")
	Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
	Data["SOURCE"] 			= "OUTPAT"
	Data["ICDVER"] 			= 8
	Data["CODE2"]			= np.NaN
	Data["CODE3"]			= np.NaN
	Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
	Data["CODE5"]			= np.NaN
	Data["CODE6"]			= np.NaN
	Data["CODE7"]			= np.NaN

	# rename columns
	Data.rename( columns = {"ADMISSION_DATE":"PVM",}, inplace=True)

	#-------------------------------------------
	# CATEGORY RESHAPE:

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names

	CATEGORY_DICTIONARY = {
	"DG1": "0",
	"DG2": "1",
	"DG3": "2",
	"DG4": "3",
	"TP1":"SFHL1",
	"TP2":"SFHL2"
	}

	new_names = Data.columns
	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in new_names]

	# perform the reshape
	VAR_FOR_RESHAPE = list( set(Data.columns)-set(new_names) )
	VAR_NOT_FOR_RESHAPE = list( set(Data.columns)-set(VAR_FOR_RESHAPE) )

	Data = pd.melt(Data,
	    id_vars 	= VAR_NOT_FOR_RESHAPE,
	    value_vars 	= VAR_FOR_RESHAPE,
	    var_name 	= "CATEGORY",
	    value_name	= "CODE1")
	Data["CATEGORY"].replace(CATEGORY_DICTIONARY, regex=True, inplace=True)

	# remove missing CODE1
	Data.dropna(subset=["CODE1"], inplace=True)
	Data.reset_index(drop=True, inplace=True)

	# SOURCE definitions
	Data["PALA"] = np.NaN
	Data["YHTEYSTAPA"] = np.NaN
	Data = Define_INPAT(Data)
	Data = Define_OPERIN(Data)
	Data = Define_OPEROUT(Data)

	# check special characters
	Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
	# special character split
	Data = CombinationCodesSplit(Data)

	# no PALTU info to add

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# check that CODE1 and 2 are not missing
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)
	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,"CODE4"] = np.NaN

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)

	# WRITE TO DETAILED LONGITUDINAL
	if test: 
		Write2TestFile(Data,header=True)
	else: 		
		Write2DetailedLongitudinal(Data,header=True)



def Hilmo_87_93_processing(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process the Hilmo information from 1987 to 1993.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

    dtypes = {
    "TNRO": str,
    "TUPVA":str, 
    "LPVM": str,
	"PDG":  str,
	"SDG1": str,
	"SDG2": str,
	"SDG3": str,
	"TMP1": str,
	"TMP2": str,
	"EDIA": str,
	"EA":   str,
	"PALTU":str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# add date of birth
	Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
	Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

	# format date columns (birth and death date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce" )
	Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce")
	# format date columns (patient in and out dates)
	Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10),  format="%d.%m.%Y",errors="coerce" )
	Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

	# check if event is after death
	Data.loc[Data.ADMISSION_DATE > Data.DEATH_DATE,"ADMISSION_DATE"] = Data.DEATH_DATE

	#-------------------------------------------
	# define columns for detailed longitudinal

	Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
	Data["EVENT_YRMNTH"]	= Data.ADMISSION_DATE.dt.strftime("%Y-%m")
	Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
	Data["SOURCE"] 			= "OUTPAT"
	Data["ICDVER"] 			= 9
	Data["CODE2"]			= np.NaN
	Data["CODE3"]			= np.NaN
	Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
	# CODE5 should be PALA but is not in columns ..
	Data["CODE5"]			= np.NaN

	#rename columns
	Data.rename( 
		columns = {
		"ADMISSION_DATE":"PVM",
		#"PALA":"CODE5",
		"EA":"CODE6",
		"PALTU":"CODE7"
		},
		inplace=True)

	#-------------------------------------------
	# CATEGORY RESHAPE:

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transferred under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names  

	CATEGORY_DICTIONARY = {
	"PDG": "0",
	"SDG1": "1",
	"SDG2": "2",
	"SDG3": "3",
	"TMP1":"MFHL1",
	"TMP2":"MFHL2",
	"EDIA":"EX"
	}

	new_names = Data.columns
	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in new_names]

	# perform the reshape
	VAR_FOR_RESHAPE = list( set(Data.columns)-set(new_names) )
	VAR_NOT_FOR_RESHAPE = list( set(Data.columns)-set(VAR_FOR_RESHAPE) )

	Data = pd.melt(Data,
	    id_vars 	= VAR_NOT_FOR_RESHAPE,
	    value_vars 	= VAR_FOR_RESHAPE,
	    var_name 	= "CATEGORY",
	    value_name	= "CODE1")
	Data["CATEGORY"].replace(CATEGORY_DICTIONARY, regex=True, inplace=True)

	# remove missing CODE1
	Data.dropna(subset=["CODE1"], inplace=True)
	Data.reset_index(drop=True, inplace=True)

	# SOURCE definitions
	Data["PALA"] = np.NaN
	Data["YHTEYSTAPA"] = np.NaN
	Data = Define_INPAT(Data)
	Data = Define_OPERIN(Data)
	Data = Define_OPEROUT(Data)

	# check special characters
	Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
	# special character split
	Data = CombinationCodesSplit(Data)

	# PALTU mapping
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=",")
	Data["CODE7"] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
	Data["CODE7"] = Data["hospital_type"]

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# check that CODE1 and 2 are not missing
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)
	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,"CODE4"] = np.NaN

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	
		Write2TestFile(Data)
	else: 		
		Write2DetailedLongitudinal(Data)



def Hilmo_94_95_processing(file_path:str, DOB_map, extra_to_merge, file_sep=";", test=False):
	"""Process the Hilmo information from 1994 to 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """

    dtypes = {
    "TNRO": str,
	"HILMO_ID": str,
    "TUPVA":str, 
    "LPVM": str,
	"PDG":  str,
	"SDG1": str,
	"SDG2": str,
	"TMP1": str,
	"TMP2": str,
	"TMP3": str,
	"PALA": str,
	"EA":   str,
	"PALTU":str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# add date of birth
	Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
	Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

	# format date columns (birth and death date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce" )
	Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce" )
	# format date columns (patient in and out dates)
	Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10),  format="%d.%m.%Y",errors="coerce" )
	Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )
	
	# check if event is after death
	Data.loc[Data.ADMISSION_DATE > Data.DEATH_DATE,"ADMISSION_DATE"] = Data.DEATH_DATE

	#-------------------------------------------
	# define columns for detailed longitudinal

	Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
	Data["EVENT_YRMNTH"]	= Data.ADMISSION_DATE.dt.strftime("%Y-%m")
	Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
	Data["SOURCE"] 			= "OUTPAT"
	Data["ICDVER"] 			= 9
	Data["CODE2"]			= np.NaN
	Data["CODE3"]			= np.NaN
	Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days

	#rename columns
	Data.rename( 
		columns = {
		"ADMISSION_DATE":"PVM",
		"PALA":"CODE5",
		"EA":"CODE6",
		"PALTU":"CODE7"
		},
		inplace=True)

	#-------------------------------------------
	# CATEGORY RESHAPE:

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names  

	CATEGORY_DICTIONARY = {
	"PDG": "0",
	"SDG1": "1",
	"SDG2": "2",
	"TMP1":"MFHL1",
	"TMP2":"MFHL2",
	"TMP3":"MFHL3"
	 }

	new_names = Data.columns
	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in new_names]

	# perform the reshape
	VAR_FOR_RESHAPE = list( set(Data.columns)-set(new_names) )
	VAR_NOT_FOR_RESHAPE = list( set(Data.columns)-set(VAR_FOR_RESHAPE) )

	Data = pd.melt(Data,
	    id_vars 	= VAR_NOT_FOR_RESHAPE,
	    value_vars 	= VAR_FOR_RESHAPE,
	    var_name 	= "CATEGORY",
	    value_name	= "CODE1")
	Data["CATEGORY"].replace(CATEGORY_DICTIONARY, regex=True, inplace=True)

	# remove missing CODE1
	Data.dropna(subset=["CODE1"], inplace=True)
	Data.reset_index(drop=True, inplace=True)	

	# merge CODE1 and CATEGORY from extra file
	Data.rename( columns = {"CATEGORY":"CATEGORY_orig","CODE1":"CODE1_orig"}, inplace=True)
	Data = Data.merge(extra_to_merge, on = "HILMO_ID", how="left")
	Data["CATEGORY"] = np.where(Data.CATEGORY_orig=="" , Data.CATEGORY, Data.CATEGORY_orig)
	Data["CODE1"]	 = np.where(Data.CODE1_orig=="" , Data.CODE1, Data.CODE1_orig)

	#------------------------------------------
	# SOURCE definitions
	Data["PALA"] = Data["CODE5"]
	Data["YHTEYSTAPA"] = np.NaN
	Data = Define_INPAT(Data)
	Data = Define_OPERIN(Data)
	Data = Define_OPEROUT(Data)

	# check special characters
	Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
	# special character split
	Data = CombinationCodesSplit(Data)

	# PALTU mapping
	paltu_map = pd.read_csv("PALTU_mapping.csv",sep=",")
	Data["CODE7"] = pd.to_numeric(Data.CODE7)
	Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
	Data["CODE7"] = Data["hospital_type"]

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# check that CODE1 and 2 are not missing
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)
	# if negative hospital days than missing value
	Data.loc[Data.CODE4<0,"CODE4"] = np.NaN

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	
		Write2TestFile(Data)
	else: 		
		Write2DetailedLongitudinal(Data)



def Hilmo_96_18_processing(file_path:str, DOB_map, extra_to_merge, file_sep=";", test=False):
	"""Process the Hilmo information after 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting t in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """

    dtypes = {
	"HILMO_ID": str,
    "TNRO": str,
    "TUPVA":str, 
    "LPVM": str,
	"PTMPK1":str,
	"PTMPK2":str,
	"PTMPK3":str,
	"MTMP1K1":str,
	"MTMP2K1":str,
	"PALA": str,
	"EA":   str,
	"PALTU":str
    }
	
	chunksize = 10 ** 6
	with pd.read_csv(file_path, chunksize=chunksize, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys()) as reader:
    	for Data in reader:

			# remove wrong codes
			wrong_codes = ["H","M","N","Z6","ZH","ZZ"]
			Data.loc[~Data.PALA.isin(wrong_codes),].reset_index(drop=True,inplace=True)

			# add date of birth
			Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
			Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

			# format date columns (birth and death date)
			Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce" )
			Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce" )
			# format date columns (patient in and out dates)
			Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )
			Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

			#-------------------------------------------
			# define columns for detailed longitudinal

			Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
			Data["EVENT_YRMNTH"]	= Data.ADMISSION_DATE.dt.strftime("%Y-%m")
			Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
			Data["SOURCE"] 			= "OUTPAT"
			Data["CODE2"]			= np.NaN
			Data["CODE3"]			= np.NaN
			Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
			Data["ICDVER"] 			= 10

			#rename columns
			Data.rename( 
				columns = {
				"ADMISSION_DATE":"PVM",
				"PALA":"CODE5",
				"EA":"CODE6",
				"PALTU":"CODE7"
				},
				inplace=True)

			#-------------------------------------------
			# CATEGORY RESHAPE:

			# the following code will reshape the Dataframe from wide to long
			# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
			# the CATEGORY names are going to be remapped to the desired names 

			CATEGORY_DICTIONARY = {
			"PTMPK1":"NOM1",
			"PTMPK2":"NOM2",
			"PTMPK3":"NOM3",
			"MTMP1K1":"NOM4",
			"MTMP2K1":"NOM5"
			 }

			new_names = Data.columns
			for name in CATEGORY_DICTIONARY.keys():
		    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in new_names]

			# perform the reshape
			VAR_FOR_RESHAPE = list( set(Data.columns)-set(new_names) )
			VAR_NOT_FOR_RESHAPE = list( set(Data.columns)-set(VAR_FOR_RESHAPE) )

			Data = pd.melt(Data,
			    id_vars 	= VAR_NOT_FOR_RESHAPE,
			    value_vars 	= VAR_FOR_RESHAPE,
			    var_name 	= "CATEGORY",
			    value_name	= "CODE1")
			Data["CATEGORY"].replace(CATEGORY_DICTIONARY, regex=True, inplace=True)

			# remove missing CODE1
			Data.dropna(subset=["CODE1"], inplace=True)
			Data.reset_index(drop=True, inplace=True)

			#-------------------------------------------

			# SOURCE definitions
			Data["PALA"] = Data["CODE5"]
			Data["YHTEYSTAPA"] = np.NaN
			Data = Define_INPAT(Data)
			Data = Define_OPERIN(Data)
			Data = Define_OPEROUT(Data)

			# merge CODE1 and CATEGORY from extra file
			Data.rename( columns = {"CATEGORY":"CATEGORY_orig","CODE1":"CODE1_orig"}, inplace=True)
			Data = Data.merge(extra_to_merge, on = "HILMO_ID", how="left")
			Data["CATEGORY"] = np.where(Data.CATEGORY_orig=="" , Data.CATEGORY, Data.CATEGORY_orig)
			Data["CODE1"]	 = np.where(Data.CODE1_orig=="" , Data.CODE1, Data.CODE1_orig)

			#-------------------------------------------

			# check special characters
			Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
			# special character split
			Data = CombinationCodesSplit(Data)

			# PALTU mapping
			paltu_map = pd.read_csv("PALTU_mapping.csv",sep=",")
			Data["CODE7"] = pd.to_numeric(Data.CODE7)
			Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
			# correct missing PALTU
			Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
			Data["CODE7"] = Data["hospital_type"]

			#-------------------------------------------
			# QUALITY CONTROL:

			# check that EVENT_AGE is in predefined range 
			Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
			# check that EVENT_AGE is not missing
			Data.dropna(subset=["EVENT_AGE"], inplace=True)
			Data.reset_index(drop=True,inplace=True)
			# check that CODE1 and 2 are not missing
			Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
			# remove duplicates
			Data.drop_duplicates(keep="first", inplace=True)
			# if negative hospital days than missing value
			Data.loc[Data.CODE4<0,"CODE4"] = np.NaN

			# select desired columns 
			Data = Data[ COLUMNS_2_KEEP ]

			# sort data
			Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

			# WRITE TO DETAILED LONGITUDINAL
			if test: 	
				Write2TestFile(Data)
			else: 		
				Write2DetailedLongitudinal(Data)



def Hilmo_POST18_processing(file_path:str, DOB_map, extra_to_merge, file_sep=";", test=False):
	"""Process the Hilmo information after 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting t in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """

    dtypes = {
	"HILMO_ID": str,
    "TNRO": str,
    "TUPVA":str, 
    "LPVM": str,
	"PTMPK1":str,
	"PTMPK2":str,
	"PTMPK3":str,
	"MTMP1K1":str,
	"MTMP2K1":str,
	"PALA": str,
	"EA":   str,
	"PALTU":str,
	"YHTEYSTAPA": str
    }

	# fetch Data
	chunksize = 10 ** 6
	with pd.read_csv(file_path, chunksize=chunksize, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys()) as reader:
    	for Data in reader:
			
			# remove wrong codes
			wrong_codes = ["H","M","N","Z6","ZH","ZZ"]
			Data.loc[~Data.PALA.isin(wrong_codes),].reset_index(drop=True,inplace=True)

			# add date of birth
			Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
			Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

			# format date columns (birth and death date)
			Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce" )
			Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce" )
			# format date columns (patient in and out dates)
			Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )
			Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

			#-------------------------------------------
			# define columns for detailed longitudinal

			Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
			Data["EVENT_YRMNTH"]	= Data.ADMISSION_DATE.dt.strftime("%Y-%m")
			Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
			Data["SOURCE"] 			= "OUTPAT"
			Data["CODE2"]			= np.NaN
			Data["CODE3"]			= np.NaN
			Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
			Data["ICDVER"] 			= 10

			#rename columns
			Data.rename( 
				columns = {
				"ADMISSION_DATE":"PVM",
				"PALA":"CODE5",
				"EA":"CODE6",
				"PALTU":"CODE7"
				},
				inplace=True)

			#-------------------------------------------
			# CATEGORY RESHAPE:

			# the following code will reshape the Dataframe from wide to long
			# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
			# the CATEGORY names are going to be remapped to the desired names 

			CATEGORY_DICTIONARY = {
			"PTMPK1":"NOM1",
			"PTMPK2":"NOM2",
			"PTMPK3":"NOM3",
			"MTMP1K1":"NOM4",
			"MTMP2K1":"NOM5"
			 }

			new_names = Data.columns
			for name in CATEGORY_DICTIONARY.keys():
		    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in new_names]

			# perform the reshape
			VAR_FOR_RESHAPE = list( set(Data.columns)-set(new_names) )
			VAR_NOT_FOR_RESHAPE = list( set(Data.columns)-set(VAR_FOR_RESHAPE) )

			Data = pd.melt(Data,
			    id_vars 	= VAR_NOT_FOR_RESHAPE,
			    value_vars 	= VAR_FOR_RESHAPE,
			    var_name 	= "CATEGORY",
			    value_name	= "CODE1")
			Data["CATEGORY"].replace(CATEGORY_DICTIONARY, regex=True, inplace=True)

			# remove missing CODE1
			Data.dropna(subset=["CODE1"], inplace=True)
			Data.reset_index(drop=True, inplace=True)	

			# merge CODE1 and CATEGORY from extra file
			Data.rename( columns = {"CATEGORY":"CATEGORY_orig","CODE1":"CODE1_orig"}, inplace=True)
			Data = Data.merge(extra_to_merge, on = "HILMO_ID", how="left")
			Data["CATEGORY"] = np.where(Data.CATEGORY_orig=="" , Data.CATEGORY, Data.CATEGORY_orig)
			Data["CODE1"]	 = np.where(Data.CODE1_orig=="" , Data.CODE1, Data.CODE1_orig)

			#-------------------------------------------
			
			# SOURCE definitions
			Data["PALA"] = Data["CODE5"]
			Data["YHTEYSTAPA"] = np.NaN
			Data = Define_INPAT(Data)
			Data = Define_OPERIN(Data)
			Data = Define_OPEROUT(Data)

			# merge CODE1 and CATEGORY from extra file
			ToAdd = Data.merge(extra_to_merge, on = "HILMO_ID", how="inner")
			#rename columns
			ToAdd.rename( 
				columns = {
				"CATEGORY":"CATEGORY_y",
				"CODE1":"CODE1_y",
				},
				inplace=True)
			ToAdd.drop(columns=['CATEGORY_x','CODE1_x'])
			Data = pd.concat([Data,ToAdd])

			#-------------------------------------------

			# check special characters
			Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
			# special character split
			Data = CombinationCodesSplit(Data)

			# PALTU mapping
			paltu_map = pd.read_csv("PALTU_mapping.csv",sep=",")
			Data["CODE7"] = pd.to_numeric(Data.CODE7)
			Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
			# correct missing PALTU
			Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
			Data["CODE7"] = Data["hospital_type"]

			#-------------------------------------------
			# QUALITY CONTROL:

			# check that EVENT_AGE is in predefined range 
			Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
			# check that EVENT_AGE is not missing
			Data.dropna(subset=["EVENT_AGE"], inplace=True)
			Data.reset_index(drop=True,inplace=True)
			# check that CODE1 and 2 are not missing
			Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
			# remove duplicates
			Data.drop_duplicates(keep="first", inplace=True)
			# if negative hospital days than missing value
			Data.loc[Data.CODE4<0,"CODE4"] = np.NaN

			# select desired columns 
			Data = Data[ COLUMNS_2_KEEP ]

			# sort data
			Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

			# WRITE TO DETAILED LONGITUDINAL
			if test: 	
				Write2TestFile(Data)
			else: 		
				Write2DetailedLongitudinal(Data)


def Hilmo_diagnosis_preparation(file_path:str,file_sep=";", test=False):
	"""Process Hilmo diagnosis.

    This function reads and processes an Hilmo diagnosis codes that need to be joined to hilmo starting from 1996.
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.

    """

    dtypes = {
    "HILMO_ID": str,
    "KENTTA": str,
    "N": int,
    "KOODI": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())


	# keep only the main ICD diagnosis code and 3 extra ones 
	Data = Data.loc[Data.KENTTA.isin(["PDGO","SDGO"])]
	Data.reset_index(drop=True,inplace=True)
	Data = Data.loc[Data.N<=3]
	Data.reset_index(drop=True,inplace=True)

	# rename columns
	Data.rename( 
		columns = {
		"N":"CATEGORY",
		"KOODI":"CODE1",
		},
		inplace=True)

	# keep only columns of interest
	Data = Data[ ["HILMO_ID","CATEGORY","CODE1"] ]

	return Data



def Hilmo_operations_preparation(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process Hilmo surgical operations.

    This function reads and processes an Hilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

    dtypes = {
    "HILMO_ID": str,
    "N": int,
    "TOIMP": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# keep only the main ICD diagnosis code and 3 extra ones 
	Data = Data.loc[Data.N<=3]
	Data.reset_index(drop=True,inplace=True)

	# rename columns
	Data.rename( 
		columns = {
		"N":"CATEGORY",
		"TOIMP":"CODE1"
		},
		inplace=True )

	# keep only columns of interest
	Data.reset_index(drop=True,inplace=True)
	Data = Data[ ["HILMO_ID","CATEGORY","CODE1"] ]

	return Data




def Hilmo_heart_preparation(file_path:str,file_sep=";", test=False):
	"""Process Hilmo heart surgeries.

    This function reads and processes an Hilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

	keys  = ['HILMO_ID'] + ['TMPC'+str(n) for n in range(1,12)] + ['TMPTYP'+str(n) for n in range(1,4)]
	values = [str for n in range(1,15)]
	dtypes = dict(zip(keys, values))
	
	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())


	#-------------------------------------------
	# CATEGORY RESHAPE:

	# the following code will reshape the Dataframe from wide to long
	# the selected columns will be transfered under the variable CATEGORY while their values will go under the variable CODE1
	# the CATEGORY names are going to be remapped to the desired names

	
	CATEGORY_DICTIONARY = {
	"TMPTYP":"HPO",
	"TMPC":"HPN"
	}

	new_names = Data.columns
	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in new_names]

	# perform the reshape
	VAR_FOR_RESHAPE = list( set(Data.columns)-set(new_names) )
	VAR_NOT_FOR_RESHAPE = list( set(Data.columns)-set(VAR_FOR_RESHAPE) )

	Data = pd.melt(Data,
	    id_vars 	= VAR_NOT_FOR_RESHAPE,
	    value_vars 	= VAR_FOR_RESHAPE,
	    var_name 	= "CATEGORY",
	    value_name	= "CODE1")
	Data["CATEGORY"].replace(CATEGORY_DICTIONARY, regex=True, inplace=True)

	# A correction for some HPO (fully numeric codes) 
	# mixed in within NPN codes (which always start with letter A). 
	Data["CODE1"] 		= Data.CODE1.astype(str)
	Data["CATEGORY"]	= np.where(Data.CODE1.str.isnumeric(), Data.CATEGORY.replace("N", "O"), Data.CATEGORY)

	# keep only columns of interest
	Data.reset_index(drop=True,inplace=True)
	Data = Data[ ["HILMO_ID","CATEGORY","CODE1"] ]

	return Data




def AvoHilmo_icd10_preparation(file_path:str,file_sep=";", test=False):
	"""Process AvoHilmo ICD10 diagnosis.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

    dtypes = {
	"TNRO": str,
	"AVOHILMO_ID": str,
	"JARJESTYS": int,
	"ICD10": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())
	
	Data.rename( columns = {"ICD10":"CODE1"}, inplace=True )

	# define the category column 
	Data["CATEGORY"] = np.NaN
	rows_to_change = Data.CODE1.notna()
	Data.loc[rows_to_change,"CATEGORY"] = "ICD" + Data.loc[rows_to_change,"JARJESTYS"].astype("string")

	# filter data
	Data.loc[ ~( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 

	# remove ICD code dots
	Data.loc[Data.CATEGORY=="ICD","CODE1"] = Data["CODE1"].replace({".", ""})

	return Data
	



def AvoHilmo_icpc2_preparation(file_path:str,file_sep=";", test=False):
	"""Process AvoHilmo ICPC2 diagnosis.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

    dtypes = {
	"TNRO": str,
	"AVOHILMO_ID": str,
	"JARJESTYS": int,
	"ICPC2": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	Data.rename( columns = {"ICPC2":"CODE1"}, inplace=True )

	# define the category column 
	Data["CATEGORY"] = np.NaN
	rows_to_change = Data.CODE1.notna()
	Data.loc[rows_to_change,"CATEGORY"] = "ICP" + Data.loc[rows_to_change,"JARJESTYS"].astype("string")

	# filter data
	Data.loc[ ~( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 

	return Data



def AvoHilmo_dental_measures_preparation(file_path:str,file_sep=";", test=False):
	"""Process AvoHilmo oral operation.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

    dtypes = {
	"TNRO": str,
	"AVOHILMO_ID": str,
	"JARJESTYS": int,
	"TOIMENPIDE": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	Data.rename( columns = {"TOIMENPIDE":"CODE1"}, inplace=True )

	# define the category column 
	Data["CATEGORY"] = np.NaN
	rows_to_change = Data.CODE1.notna()
	Data.loc[rows_to_change,"CATEGORY"] = "MOP" + Data.loc[rows_to_change,"JARJESTYS"].astype("string")

	# filter data
	Data.loc[ ~( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 

	return Data



def AvoHilmo_interventions_preparation(file_path:str,file_sep=";", test=False):
	"""Process AvoHilmo surgery operations.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

    dtypes = {
	"TNRO": str,
	"AVOHILMO_ID": str,
	"JARJESTYS": int,
	"TOIMENPIDE": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	Data.rename( columns = {"TOIMENPIDE":"CODE1"},inplace=True )

	# define the category column 
	Data["CATEGORY"] = np.NaN
	rows_to_change = Data.CODE1.notna()
	Data.loc[rows_to_change,"CATEGORY"] = "OP" + Data.loc[rows_to_change,"JARJESTYS"].astype("string")

	# filter data
	Data.loc[ ~( Data.CODE1.isna() & Data.CATEGORY.isna() & Data.JARJESTYS.isna() )].reset_index(drop=True,inplace=True) 

	return Data



def AvoHilmo_processing(file_path:str, DOB_map, extra_to_merge, file_sep=";", test=False):
	"""Process AvoHilmo general file.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    also reads an extra AvoHilmo dataset about diagnosis/operations in order to add CODE1 and CATEGORY columns. 
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
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

    dtypes = {
	"TNRO": str,
	"AVOHILMO_ID": str,
	"KAYNTI_ALKOI":str,
	"KAYNTI_YHTEYSTAPA":str,
	"KAYNTI_PALVELUMUOTO":str,
	"KAYNTI_AMMATTI":str,
    }

	# fetch Data
	chunksize = 10 ** 6
	with pd.read_csv(file_path, chunksize=chunksize, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys()) as reader:
    	for Data in reader:

			# add date of birth
			Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
			Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

			# format date columns 
			Data["EVENT_DATE"] 		= pd.to_datetime( Data["KAYNTI_ALKOI"].str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )
			# format date columns (birth and death date)
			Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce" )
			Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce" )

			# check if event is after death
			Data.loc[Data.EVENT_DATE > Data.DEATH_DATE,"EVENT_DATE"] = Data.DEATH_DATE

			#-------------------------------------------
			# define columns for detailed longitudinal
			Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
			Data["EVENT_YRMNTH"]	= Data.EVENT_DATE.dt.strftime("%Y-%m")
			Data["EVENT_YEAR"]		= Data.EVENT_DATE.dt.year
			Data["ICDVER"]			= 10
			Data["SOURCE"]			= "PRIM_OUT"
			Data["INDEX"]			= Data.AVOHILMO_ID
			Data["CODE2"]			= np.NaN
			Data["CODE3"]			= np.NaN
			Data["CODE4"]			= np.NaN

			# rename columns
			Data.rename( 
				columns = {
				"KAYNTI_YHTEYSTAPA":"CODE5",
				"KAYNTI_PALVELUMUOTO":"CODE6",
				"KAYNTI_AMMATTI":"CODE7",
				"EVENT_DATE":"PVM"},
				inplace=True )

			#-------------------------------------------

			# merge CODE1 and CATEGORY from extra file
			Data = Data.merge(extra_to_merge, on = "AVOHILMO_ID", how="left")

			# PALTU mapping
			paltu_map = pd.read_csv("PALTU_mapping.csv",sep=",")
			Data["CODE7"] = pd.to_numeric(Data.CODE7)
			Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU", how="left")
			# correct missing PALTU
			Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
			Data["CODE7"] = Data["hospital_type"]

			#-------------------------------------------
			# QUALITY CONTROL:

			# check that EVENT_AGE is in predefined range 
			Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
			# check that EVENT_AGE is not missing
			Data.dropna(subset=["EVENT_AGE"], inplace=True)
			Data.reset_index(drop=True,inplace=True)
			# check that CODE1 and 2 are not missing
			Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
			# remove duplicates
			Data.drop_duplicates(keep="first", inplace=True)

			# select desired columns 
			Data = Data[ COLUMNS_2_KEEP ]

			# sort data
			Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

			# WRITE TO DETAILED LONGITUDINAL
			if test: 	Write2TestFile(Data)
			else: 		Write2DetailedLongitudinal(Data)




def DeathRegistry_processing(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process the information from death registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

    dtypes = {
	"TNRO": str,
	"KPV": str,
	"VKS": str,
	"M1": str,
	"M2": str,
	"M3": str,
	"M4": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# add date of birth
	Data = Data.merge(DOB_map, left_on = "TNRO",right_on = "FINREGISTRYID")
	Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )
	# format date columns (birth date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE.str.slice(stop=10), format="%Y-%m-%d" )
	Data["EVENT_DATE"]		= pd.to_datetime( Data.KPV.str.slice(stop=10), format="%d.%m.%Y" )

	# define columns for detailed longitudinal
	Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
	Data["EVENT_YEAR"] 		= Data.EVENT_DATE.dt.year	
	Data["EVENT_YRMNTH"]	= Data.EVENT_DATE.dt.strftime("%Y-%m")
	Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
	Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
	Data["SOURCE"] 			= "DEATH"
	Data["CODE2"]			= np.NaN
	Data["CODE3"]			= np.NaN
	Data["CODE4"]			= np.NaN
	Data["CODE5"]			= np.NaN
	Data["CODE6"]			= np.NaN
	Data["CODE7"]			= np.NaN

	# rename columns
	Data.rename( 
		columns = {
		"EVENT_DATE":"PVM"
		},
		inplace=True)

	#-------------------------------------------
	# CATEGORY RESHAPE:

	# the following code will reshape the Dataframe from wide to long
	# if there was a value under one of the category columns this will be transferred under the column CODE1
	# NB: the category column values are going to be remapped after as desired  

	CATEGORY_DICTIONARY	= {
	"TPKS":"U",
	"VKS":"I",
	"M1":"c1",
	"M2":"c2",
	"M3":"c3",
	"M4":"c4"
	}

	new_names = Data.columns
	for name in CATEGORY_DICTIONARY.keys():
    	new_names = [s.replace(name, CATEGORY_DICTIONARY[name]) for s in new_names]

    # perform the reshape
	VAR_FOR_RESHAPE = list( set(Data.columns)-set(new_names) )
	VAR_NOT_FOR_RESHAPE = list( set(Data.columns)-set(VAR_FOR_RESHAPE) )

	Data = pd.melt(Data,
	    id_vars 	= VAR_NOT_FOR_RESHAPE,
	    value_vars 	= VAR_FOR_RESHAPE,
	    var_name 	= "CATEGORY",
	    value_name	= "CODE1")
	Data["CATEGORY"].replace(CATEGORY_DICTIONARY, regex=True, inplace=True)

	# remove missing CODE1
	Data.dropna(subset=["CODE1"], inplace=True)
	Data.reset_index(drop=True, inplace=True)

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)

	# NOT performing code check in this registry

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	
		Write2TestFile(Data)
	else: 		
		Write2DetailedLongitudinal(Data)



def CancerRegistry_processing(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process the information from cancer registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

    dtypes = {
	"FINREGISTRYID": str,
	"dg_date":str,
	"topo": str,
	"morpho": str,
	"beh": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# add date of birth
	Data = Data.merge(DOB_map, on = "FINREGISTRYID")
	Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

	# format date columns (birth and death date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d",errors="coerce" )
	Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d",errors="coerce" )
	# format date columns (diagnosis date)
	Data["EVENT_DATE"]		= pd.to_datetime( Data.dg_date, format="%Y-%m-%d", errors="coerce" )

	#-------------------------------------------
	# define columns for detailed longitudinal

	Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
	Data["EVENT_YEAR"] 		= Data.EVENT_DATE.dt.year	
	Data["EVENT_YRMNTH"]	= Data.EVENT_DATE.dt.strftime("%Y-%m")
	Data["ICDVER"] 			= "O3"
	Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
	Data["SOURCE"] 			= "CANC"
	Data["CATEGORY"] 		= np.NaN
	Data["CODE4"]			= np.NaN
	Data["CODE5"]			= np.NaN
	Data["CODE6"]			= np.NaN
	Data["CODE7"]			= np.NaN

	# rename columns
	Data.rename( 
		columns = {
		"topo":"CODE1",
		"morpho":"CODE2",
		"beh":"CODE3",
		"EVENT_DATE":"PVM"
		},
		inplace=True)

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# check that CODE1 and 2 are not missing
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	
		Write2TestFile(Data)
	else: 		
		Write2DetailedLongitudinal(Data)



def KelaReimbursement_PRE20_processing(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process the information from kela reimbursement registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

    dtypes = {
	"HETU": str,
	"ALPV": str,
	"LOPV": str,
	"SK1":  str,
	"DIAG": str
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# add date of birth
	Data = Data.merge(DOB_map, left_on = "HETU",right_on = "FINREGISTRYID")
	Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

	# format date columns (birth and death date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce")
	Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce")
	# format date columns (reimbursement date)
	Data["REIMB_START"]		= pd.to_datetime( Data.ALPV, format="%Y-%m-%d", errors="coerce")
	Data["REIMB_END"]		= pd.to_datetime( Data.LOPV, format="%Y-%m-%d", errors="coerce")

	#-------------------------------------------
	# define columns for detailed longitudinal

	Data["EVENT_AGE"] 		= round( (Data.REIMB_START - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)
	Data["EVENT_YEAR"] 		= Data.REIMB_START.dt.year
	Data["EVENT_YRMNTH"]	= Data.REIMB_START.dt.strftime("%Y-%m")
	Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
	Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
	Data["SOURCE"] 			= "REIMB"
	Data["CATEGORY"] 		= np.NaN
	Data["CODE3"]			= np.NaN
	Data["CODE4"]			= np.NaN
	Data["CODE5"]			= np.NaN
	Data["CODE6"]			= np.NaN
	Data["CODE7"]			= np.NaN

	#rename columns
	Data.rename(
		columns = {
		"SK1":"CODE1",
		"DIAG":"CODE2",
		"REIMB_START":"PVM"
		}, 
		inplace = True )

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# check that CODE1 and 2 are not missing
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)

	# remove ICD code dots
	Data["CODE2"] = Data["CODE2"].replace({".", ""})

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	
		Write2TestFile(Data)
	else: 		
		Write2DetailedLongitudinal(Data)

def KelaReimbursement_20_21_processing(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process the information from kela reimbursement registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
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
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1")

	# add date of birth
	Data.columns = ["HETU"] + list(Data.columns[1:])
	Data = Data.merge(DOB_map, left_on = "HETU",right_on = "FINREGISTRYID")
	Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )

	# format date columns (birth and death date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce")
	Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce")
	# format date columns (reimbursement date)
	Data["REIMB_START"]		= pd.to_datetime( Data.korvausoikeus_alpv, format="%Y-%m-%d", errors="coerce")
	Data["REIMB_END"]		= pd.to_datetime( Data.korvausoikeus_lopv, format="%Y-%m-%d", errors="coerce")

	#-------------------------------------------
	# define columns for detailed longitudinal

	Data["EVENT_AGE"] 		= round( (Data.REIMB_START - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)
	Data["EVENT_YEAR"] 		= Data.REIMB_START.dt.year
	Data["EVENT_YRMNTH"]	= Data.REIMB_START.dt.strftime("%Y-%m")
	Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
	Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
	Data["SOURCE"] 			= "REIMB"
	Data["CATEGORY"] 		= np.NaN
	Data["CODE3"]			= np.NaN
	Data["CODE4"]			= np.NaN
	Data["CODE5"]			= np.NaN
	Data["CODE6"]			= np.NaN
	Data["CODE7"]			= np.NaN

	#rename columns
	Data.rename(
		columns = {
		"KORVAUSOIKEUS_KOODI":"CODE1",
		"DIAGNOOSI_KOODI":"CODE2",
		"REIMB_START":"PVM"
		}, 
		inplace = True )

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# check that CODE1 and 2 are not missing
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)

	# remove ICD code dots
	Data["CODE2"] = Data["CODE2"].replace({".", ""})

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	Write2TestFile(Data)
	else: 		Write2DetailedLongitudinal(Data)	



def KelaPurchase_processing(file_path:str, DOB_map, file_sep=";", test=False):
	"""Process the information from kela purchases registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe, optional): dataframe mapping DOB codes to their corresponding dates
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
        ValueError: If the provided DOB_map is not a pandas DataFrame.
    """	

    dtypes = {
	"HETU": str,
	"OSTOPV": str,
	"ATC":  str,
	"SAIR": str,
	"VNRO": str,
	"PLKM": str,
	"KORV_EUR": str,
	"KAKORV_EUR": str,
	"LAJI": str,
    }

	# fetch Data
	if test: 	
		Data = pd.read_csv(file_path, nrows=5000, sep = file_sep, encoding="latin-1")		
	else: 		
		Data = pd.read_csv(file_path, sep = file_sep, encoding="latin-1", dtype=dtypes, usecols=dtypes.keys())

	# add date of birth
	Data = Data.merge(DOB_map,left_on = "HETU",right_on = "FINREGISTRYID")
	Data.rename( columns = {"date_of_birth":"BIRTH_DATE"}, inplace = True )
	# format date columns (birth and death date)
	Data["BIRTH_DATE"] 		= pd.to_datetime( Data.BIRTH_DATE, format="%Y-%m-%d", errors="coerce")
	Data["DEATH_DATE"] 		= pd.to_datetime( Data.death_date, format="%Y-%m-%d", errors="coerce")
	# format date columns (purchase date)
	Data["EVENT_DATE"] 		= pd.to_datetime( Data.OSTOPV, format="%Y-%m-%d", errors="coerce") 	

	#-------------------------------------------	
	# define columns for detailed longitudinal

	Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)
	Data["EVENT_YEAR"] 		= Data.EVENT_DATE.dt.year
	Data["EVENT_YRMNTH"]	= Data.EVENT_DATE.dt.strftime("%Y-%m")
	Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
	Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
	Data["SOURCE"] 			= "PURCH"
	Data["CATEGORY"] 		= np.NaN

	#rename columns
	Data.rename(
		columns = {
		"ATC":"CODE1",
		"SAIR":"CODE2",
		"VNRO":"CODE3",
		"PLKM":"CODE4",
		"KORV_EUR":"CODE5",
		"KAKORV_EUR":"CODE6",
		"LAJI":"CODE7",
		"EVENT_DATE":"PVM"
		},
		inplace = True )

	#-------------------------------------------
	# QUALITY CONTROL:

	# check that EVENT_AGE is in predefined range 
	Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110), ].reset_index(drop=True,inplace=True)
	# check that EVENT_AGE is not missing
	Data.dropna(subset=["EVENT_AGE"], inplace=True)
	Data.reset_index(drop=True,inplace=True)
	# check that CODE1 and 2 are not missing
	Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ,].reset_index(drop=True,inplace=True) 
	# remove duplicates
	Data.drop_duplicates(keep="first", inplace=True)

	# remove dot in VNRO code
	Data["CODE3"] = Data.CODE3.astype("string").str.split(".",expand=True)[0]
	# complete VNR code if shorter than 6 digits
	MISSING_DIGITS = 6 - Data.CODE3.str.len()
	ZERO = pd.Series( ["0"] * Data.shape[0] ).astype("string")
	Data["CODE3"] = Data.CODE3 + ZERO*MISSING_DIGITS

	# select desired columns 
	Data = Data[ COLUMNS_2_KEEP ]

	# sort data
	Data.sort_values(by = ["FINREGISTRYID","EVENT_AGE"], inplace=True)	

	# WRITE TO DETAILED LONGITUDINAL
	if test: 	
		Write2TestFile(Data)
	else: 		
		Write2DetailedLongitudinal(Data)

