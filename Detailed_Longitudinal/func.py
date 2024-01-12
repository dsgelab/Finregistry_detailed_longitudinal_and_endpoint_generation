
##########################################################
# COPYRIGHT:  	THL/FIMM/Finregistry  2023
# AUTHORS:     	Matteo Ferro, Essi Viippola
##########################################################

# LIBRARIES

import re
import os
import pandas as pd
import numpy as np
from datetime import datetime as dt
from pathlib import Path

from config import DETAILED_LONGITUDINAL_PATH, TEST_FOLDER_PATH


##########################################################
# UTILITY VARIABLES

DAYS_TO_YEARS = 365.24
#NB: copied from FinnGen

PALA_INPAT_LIST= [1,3,4,5,6,7,8,31]

OUTPUT_COLUMNS = {
    "FINREGISTRYID":str,
    "SOURCE":str,
    "EVENT_AGE":float,
    "EVENT_DAY":str,
    "CODE1":str,
    "CODE2":str, 
    "CODE3":str, 
    "CODE4":str, 
    "CODE5":str,
    "CODE6":str, 
    "CODE7":str,
    "CODE8":str,
    "CODE9":str,
    "ICDVER":str,
    "CATEGORY":str,
    "INDEX":int}

COLUMNS_2_KEEP = list(OUTPUT_COLUMNS.keys())
COLUMNS_DTYPES = list(OUTPUT_COLUMNS.values())

##########################################################
# UTILITY FUNCTIONS

def DOB_map_preparation(filepath:str, sep=";"):
    """
    Prepare death and birth date information to be mapped into to the processed files for each patient
    """

    dtypes = {
    "FINREGISTRYID":str,
    "date_of_birth":str,
    "death_date":str
    }

    birth_death_map = pd.read_csv(
        filepath_or_buffer = filepath,
        sep = sep, 
        encoding = "latin-1", 
        dtype = dtypes, 
        usecols = dtypes.keys())

    birth_death_map.rename( columns = {"date_of_birth":"BIRTH_DATE","death_date":"DEATH_DATE"}, inplace = True )
    # format date columns (birth and death date)
    birth_death_map["BIRTH_DATE"] = pd.to_datetime( birth_death_map.BIRTH_DATE, format="%Y-%m-%d", errors="coerce" )
    birth_death_map["DEATH_DATE"] = pd.to_datetime( birth_death_map.DEATH_DATE, format="%Y-%m-%d", errors="coerce" )

    return birth_death_map

def read_in(file_path:str, file_sep:str, dtype:dict, test = False):
    """Read into a pandas dataframe 

    Args:
        filepath: path of the file to be processed.
        file_sep: separator used in the file.
        test (Bool): set to True if the script is ran in the testing phase
        dtypes (optional): type of the columns to fetch

    Returns:
        Fetched data  
    """

    if test: 	
        Data = pd.read_csv(file_path, sep=file_sep, encoding='latin-1', nrows=5_000)		
    else: 		
        Data = pd.read_csv(file_path, sep=file_sep, encoding='latin-1', dtype=dtype, usecols=dtype.keys())

    return Data


def read_in_chunks(file_path:str, file_sep:str, dtype:dict, chunck_size = 10**6, test = False):
    """Read into a pandas dataframe in chunks (if test=True)

    Args:
        filepath: path of the file to be processed.
        file_sep: separator used in the file.
        test (Bool): set to True if the script is ran in the testing phase
        dtypes (optional): type of the columns to fetch

    Returns:
        reader function
    """

    if test: 	
        return pd.read_csv(file_path, sep=file_sep, encoding='latin-1', chunksize=chunck_size, nrows=5_000)		
    else: 		
        return pd.read_csv(file_path, sep=file_sep, encoding='latin-1', chunksize=chunck_size, dtype=dtype, usecols=dtype.keys()) 



def write_out(Data: pd.DataFrame, output_name: str, header = False, test = False):
    """Writes pandas dataframe to detailed_longitudinal or test file

    append if already exist and also insert date in the filename

    Args:
        Data (pd.DataFrame): the dataframe to be written.
        path (str, optional): The file path to write the data to. 
                              Defaults to DETAILED_LONGITUDINAL_PATH.

    Returns:
        None 
    """

    if test: 
        path = TEST_FOLDER_PATH
        today = dt.today().strftime("%Y_%m_%d")
        filename = output_name + "_test_" + today + ".csv"

    else:
        path = DETAILED_LONGITUDINAL_PATH
        filename = output_name + ".csv"

    #remove header if file is already existing
    final_path = Path(path)/filename
    if os.path.exists(final_path): 
        header=False
    else: 
        header=True

    # Convert all columns to desired data types before exporting
    for i,col in enumerate(Data.columns):
        Data[col] = Data[col].astype(COLUMNS_DTYPES[i])
    Data.to_csv(
        path_or_buf = final_path, 
        mode="a", 
        sep=",", 
        encoding="utf-8", #same for every Finregistry file
        index=False,
        header=header
        )


def combination_codes_split(Data):
    """
    Splits combinatino codes in the input dataframe CODE1 based on special characters.
    NB: check FinnGen Analyst Handbook for more info.

    The `special_chars` dictionary specifies to which column the first and second part of the split cell goes.
    For example, code "111#111" is split by a hash so the first part goes into CODE1 and the second part to CODE3, according to the dictionary.

    Codes with multiple special characters are dropped.

    Args:
        Data (pd.DataFrame): Dataframe to be split

    Returns:
        Data (pd.DataFrame): Dataframe with combination codes split into their respective columns

    """
    # Specify special characters and their respective column positions
    special_chars = {
        "\*": ["CODE1", "CODE2"],
        "\&": ["CODE1", "CODE2"],
        "\#": ["CODE1", "CODE3"],
        "\+": ["CODE2", "CODE1"]
    }

    # Drop rows with NA as CODE1 or multiple special characters
    Data = Data.loc[
        (~Data["CODE1"].isna()) & 
        (Data["CODE1"].str.count("|".join(special_chars.keys())) < 2)]
    Data = Data.reset_index(drop=True)

    # Store the original CODE1 into a variable to avoid overwriting CODE1
    original_code = Data["CODE1"]

    # Loop through special characters and split codes into variables according to the dictionary
    for s in special_chars.keys():
        indx = original_code.str.contains(pat=s, regex=True)
        if sum(indx) > 0:
            split_codes = original_code[indx].str.split(pat=s)
            Data.loc[split_codes.index, special_chars[s]] = split_codes.tolist()

    # Replace empty strings with NA
    Data = Data.replace(r"^\s*$", np.NaN, regex=True)

    return Data


def fix_missing_value(Data:pd.DataFrame):
    """replace -1 with missing value

    checks all the columns starting with CODE and replace -1 with np.NaN

    Args:
        Data (pd.DataFrame): hilmo or avohilmo dataframe to work on.

    Returns:
        Data (pd.DataFrame): hilmo or avohilmo dataframe with correct CODEs.

    Raises:
    ValueError: If the provided Data is not a pandas DataFrame.
    """

    code_list = ["CODE"+str(n) for n in range(1,10)]
    for code in code_list:
        Data.loc[Data[code].isin(['-1',-1]), code] = np.NaN

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

    # adjust possible errors
    Data.loc[Data.PALA=="", "PALA"] = np.NaN
    Data.loc[Data.YHTEYSTAPA=="", "YHTEYSTAPA"] = np.NaN
    
    # RULE 1969-1997: all is INPAT
    Data.loc[ (Data.EVENT_DAY.dt.year<1998) ,"SOURCE"] = "INPAT"

    # RULE 1998-2018: OUTPAT depends on PALA variable
    Data.loc[ (Data.EVENT_DAY.dt.year>=1998) & (Data.EVENT_DAY.dt.year<=2018) & Data.PALA.isin(PALA_INPAT_LIST),"SOURCE"] = "INPAT"

    # RULE 2019-NOW: OUTPAT depends on PALA and YHTEYSTAPA variable
    Data.loc[ (Data.EVENT_DAY.dt.year>2018) & (Data.YHTEYSTAPA=="R80"), "SOURCE"] = "INPAT"
    Data.loc[ (Data.EVENT_DAY.dt.year>2018) & (Data.YHTEYSTAPA=="R10") & Data.PALA.isin(PALA_INPAT_LIST), "SOURCE"] = "INPAT"
    Data.loc[ (Data.EVENT_DAY.dt.year>2018) & Data.YHTEYSTAPA.isna() & Data.PALA.isin(PALA_INPAT_LIST), "SOURCE"] = "INPAT"

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
    Data = read_in(file_path, file_sep, dtype=dtypes, test=test)
    # add date of birth/death
    Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
    # format date columns (patient in and out dates)
    Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TULOPV.str.slice(stop=10),  format="%d.%m.%Y",errors="coerce" )
    Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LAHTOPV.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

    # check if discharge is after death
    Data.loc[Data.DISCHARGE_DATE > Data.DEATH_DATE,"DISCHARGE_DATE"] = Data.DEATH_DATE

    #-------------------------------------------
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
    Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
    Data["SOURCE"] 			= "OUTPAT"
    Data["ICDVER"] 			= 8
    Data["CODE2"]			= np.NaN
    Data["CODE3"]			= np.NaN
    Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
    Data["CODE5"]			= np.NaN
    Data["CODE6"]			= np.NaN
    Data["CODE7"]			= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

    # rename columns
    Data.rename( columns = {"ADMISSION_DATE":"EVENT_DAY"}, inplace=True)

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
        new_names = [CATEGORY_DICTIONARY.get(name, name) if s == name else s for s in new_names]

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
    Data = Data.dropna(subset=["CODE1"])
    Data = Data.reset_index(drop=True)

    # SOURCE definitions
    Data["PALA"] = np.NaN
    Data["YHTEYSTAPA"] = np.NaN
    Data = Define_INPAT(Data)
    Data = Define_OPERIN(Data)
    Data = Define_OPEROUT(Data)

    # check special characters
    Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
    # special character split
    Data = combination_codes_split(Data)

    # no PALTU info to add

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110)]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()]
    Data = Data.reset_index(drop=True)
    # if negative hospital days than missing value
    Data.loc[Data.CODE4<0,"CODE4"] = np.NaN
    # if -1 in a CODE column replace with missing
    Data = fix_missing_value(Data)

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="Hilmo", test=test)



def Hilmo_87_93_processing(file_path:str, DOB_map, paltu_map, file_sep=";", test=False):
    """Process the Hilmo information from 1987 to 1993.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
        paltu_map (pd.dataframe): dataframe for mapping PALTU codes to hospital name
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
    Data = read_in(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test)
    # add date of birth
    Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
    # format date columns (patient in and out dates)
    Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10),  format="%d.%m.%Y",errors="coerce" )
    Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

    # check if event is after death
    Data.loc[Data.DISCHARGE_DATE > Data.DEATH_DATE,"DISCHARGE_DATE"] = Data.DEATH_DATE

    #-------------------------------------------
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
    Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
    Data["SOURCE"] 			= "OUTPAT"
    Data["ICDVER"] 			= 9
    Data["CODE2"]			= np.NaN
    Data["CODE3"]			= np.NaN
    Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
    # CODE5 should be PALA but is not available
    Data["CODE5"]			= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

    #rename columns
    Data.rename( 
        columns = {
        "ADMISSION_DATE":"EVENT_DAY",
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
        new_names = [CATEGORY_DICTIONARY.get(name, name) if s == name else s for s in new_names]

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
    Data = Data.dropna(subset=["CODE1"])
    Data = Data.reset_index(drop=True)

    # SOURCE definitions
    Data["PALA"] = np.NaN
    Data["YHTEYSTAPA"] = np.NaN
    Data = Define_INPAT(Data)
    Data = Define_OPERIN(Data)
    Data = Define_OPEROUT(Data)

    # check special characters
    Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
    # special character split
    Data = combination_codes_split(Data)

    # PALTU mapping
    Data["CODE7"] = pd.to_numeric(Data.CODE7)
    Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
    # correct missing PALTU
    Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
    Data["CODE7"] = Data["hospital_type"]

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110)]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()]
    Data = Data.reset_index(drop=True)
    # if negative hospital days than missing value
    Data.loc[Data.CODE4<0,"CODE4"] = np.NaN
    # if -1 in a CODE column replace with missing
    Data = fix_missing_value(Data)

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="Hilmo", test=test)



def Hilmo_94_95_processing(file_path:str, DOB_map, paltu_map, extra_to_merge, file_sep=";", test=False):
    """Process the Hilmo information from 1994 to 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
        paltu_map (pd.dataframe): dataframe for mapping PALTU codes to hospital name
        extra_to_merge (pd.dataframe): dataframe with diagnosis codes to be added
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
    Data = read_in(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test)
    # add date of birth/death
    Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
    # format date columns (patient in and out dates)
    Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10),  format="%d.%m.%Y",errors="coerce" )
    Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

    # check if event is after death
    Data.loc[Data.DISCHARGE_DATE > Data.DEATH_DATE,"DISCHARGE_DATE"] = Data.DEATH_DATE

    #-------------------------------------------
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
    Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
    Data["SOURCE"] 			= "OUTPAT"
    Data["ICDVER"] 			= 9
    Data["CODE2"]			= np.NaN
    Data["CODE3"]			= np.NaN
    Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

    #rename columns
    Data.rename( 
        columns = {
        "ADMISSION_DATE":"EVENT_DAY",
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
        new_names = [CATEGORY_DICTIONARY.get(name, name) if s == name else s for s in new_names]

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
    Data = Data.dropna(subset=["CODE1"])
    Data = Data.reset_index(drop=True)

    # merge CODE1 and CATEGORY from extra file
    to_append = Data.drop(['CATEGORY','CODE1'],axis=1).merge(extra_to_merge, on = "HILMO_ID", how="left")
    Data = pd.concat([Data,to_append], ignore_index=True)

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
    Data = combination_codes_split(Data)

    # PALTU mapping
    Data["CODE7"] = pd.to_numeric(Data.CODE7)
    Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
    # correct missing PALTU
    Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
    Data["CODE7"] = Data["hospital_type"]

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110)]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()]
    Data = Data.reset_index(drop=True)
    # if negative hospital days than missing value
    Data.loc[Data.CODE4<0,"CODE4"] = np.NaN
    # if -1 in a CODE column replace with missing
    Data = fix_missing_value(Data)

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="Hilmo", test=test)



def Hilmo_96_18_processing(file_path:str, DOB_map, paltu_map, extra_to_merge, file_sep=";", test=False):
    """Process the Hilmo information after 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting t in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
        paltu_map (pd.dataframe): dataframe for mapping PALTU codes to hospital name
        extra_to_merge (pd.dataframe): dataframe with diagnosis codes to be added
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


    with read_in_chunks(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test) as reader:
        for Data in reader:

            # remove wrong codes
            wrong_codes = ["H","M","N","Z6","ZH","ZZ"]
            Data = Data.loc[~Data.PALA.isin(wrong_codes)]
            Data = Data.reset_index(drop=True)

            # add date of birth/death
            Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
            # format date columns (patient in and out dates)
            Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )
            Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

            #-------------------------------------------
            # define columns for detailed longitudinal

            Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
            Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
            Data["SOURCE"] 			= "OUTPAT"
            Data["ICDVER"] 			= 10
            Data["CODE2"]			= np.NaN
            Data["CODE3"]			= np.NaN
            Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days
            Data["CODE8"]			= np.NaN
            Data["CODE9"]			= np.NaN

            #rename columns
            Data.rename( 
                columns = {
                "ADMISSION_DATE":"EVENT_DAY",
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
                new_names = [CATEGORY_DICTIONARY.get(name, name) if s == name else s for s in new_names]


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
            Data = Data.dropna(subset=["CODE1"])
            Data = Data.reset_index(drop=True)

            # merge CODE1 and CATEGORY from extra file
            to_append = Data.drop(['CATEGORY','CODE1'],axis=1).merge(extra_to_merge, on = "HILMO_ID", how="left")
            Data = pd.concat([Data,to_append], ignore_index=True)
            #-------------------------------------------

            # SOURCE definitions
            Data["PALA"] = Data["CODE5"]
            Data["YHTEYSTAPA"] = np.NaN
            Data = Define_INPAT(Data)
            Data = Define_OPERIN(Data)
            Data = Define_OPEROUT(Data)

            # check special characters
            Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
            # special character split
            Data = combination_codes_split(Data)

            # PALTU mapping
            Data["CODE7"] = pd.to_numeric(Data.CODE7)
            Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
            # correct missing PALTU
            Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
            Data["CODE7"] = Data["hospital_type"]

            #-------------------------------------------
            # QUALITY CONTROL:

            # check that EVENT_AGE is in predefined range 
            Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110)]
            Data = Data.reset_index(drop=True)
            # check that EVENT_AGE is not missing
            Data.dropna(subset=["EVENT_AGE"])
            Data = Data.reset_index(drop=True)
            # check that CODE1 and 2 are not missing
            Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()]
            Data = Data.reset_index(drop=True)
            # if negative hospital days than missing value
            Data.loc[Data.CODE4<0,"CODE4"] = np.NaN
            # if -1 in a CODE column replace with missing
            Data = fix_missing_value(Data)

            # select desired columns 
            Data = Data[ COLUMNS_2_KEEP ]

            # WRITE TO DETAILED LONGITUDINAL
            write_out(Data, output_name="Hilmo", test=test)



def Hilmo_POST18_processing(file_path:str, DOB_map, paltu_map, extra_to_merge, file_sep=";", test=False):
    """Process the Hilmo information after 1995.

    This function reads and processes an Hilmo file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting t in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
        paltu_map (pd.dataframe): dataframe for mapping PALTU codes to hospital name
        extra_to_merge (pd.dataframe): dataframe with diagnosis codes to be added
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
    "YHTEYSTAPA": str,
    "KIIREELLISYYS":str
    }

    # fetch Data
    chunksize = 10 ** 6
    with read_in_chunks(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test) as reader:
        for Data in reader:

            # remove wrong codes
            wrong_codes = ["H","M","N","Z6","ZH","ZZ"]
            Data = Data.loc[~Data.PALA.isin(wrong_codes)]
            Data = Data.reset_index(drop=True)

            # add date of birth
            Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
            # format date columns (patient in and out dates)
            Data["ADMISSION_DATE"] 	= pd.to_datetime( Data.TUPVA.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )
            Data["DISCHARGE_DATE"]	= pd.to_datetime( Data.LPVM.str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

            #-------------------------------------------
            # define columns for detailed longitudinal

            Data["EVENT_AGE"] 		= round( (Data.ADMISSION_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
            Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
            Data["SOURCE"] 			= "OUTPAT"
            Data["ICDVER"] 			= 10
            Data["CODE2"]			= np.NaN
            Data["CODE3"]			= np.NaN
            Data["CODE4"]			= (Data.DISCHARGE_DATE - Data.ADMISSION_DATE).dt.days

            #rename columns
            Data.rename( 
                columns = {
                "ADMISSION_DATE":"EVENT_DAY",
                "PALA":"CODE5",
                "EA":"CODE6",
                "PALTU":"CODE7",
                "YHTEYSTAPA":"CODE8",
                "KIIREELLISYYS":"CODE9"
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
                new_names = [CATEGORY_DICTIONARY.get(name, name) if s == name else s for s in new_names]

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
            Data = Data.dropna(subset=["CODE1"])
            Data = Data.reset_index(drop=True)	

            # merge CODE1 and CATEGORY from extra file
            to_append = Data.drop(['CATEGORY','CODE1'],axis=1).merge(extra_to_merge, on = "HILMO_ID", how="left")
            Data = pd.concat([Data,to_append], ignore_index=True)
            #-------------------------------------------

            # SOURCE definitions
            Data["PALA"] = Data["CODE5"]
            Data["YHTEYSTAPA"] = Data["CODE8"]
            Data = Define_INPAT(Data)
            Data = Define_OPERIN(Data)
            Data = Define_OPEROUT(Data)

            # check special characters
            Data.loc[Data.CODE1.isin(["TÃ\xe2\x82", "JÃ\xe2\x82","LÃ\xe2\x82"]),"CODE1"] = np.NaN
            # special character split
            Data = combination_codes_split(Data)

            # PALTU mapping
            Data["CODE7"] = pd.to_numeric(Data.CODE7)
            Data = Data.merge(paltu_map, left_on="CODE7", right_on="PALTU")
            # correct missing PALTU
            Data.loc[ Data.CODE7.isna(),"hospital_type"] = "Other Hospital" 
            Data["CODE7"] = Data["hospital_type"]

            #-------------------------------------------
            # QUALITY CONTROL:

            # check that EVENT_AGE is in predefined range 
            Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110)]
            Data = Data.reset_index(drop=True)
            # check that EVENT_AGE is not missing
            Data = Data.dropna(subset=["EVENT_AGE"])
            Data = Data.reset_index(drop=True)
            # check that CODE1 and 2 are not missing
            Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()]
            Data = Data.reset_index(drop=True)
            # if negative hospital days than missing value
            Data.loc[Data.CODE4<0,"CODE4"] = np.NaN
            # if -1 in a CODE column replace with missing
            Data = fix_missing_value(Data)

            # select desired columns 
            Data = Data[ COLUMNS_2_KEEP ]

            # WRITE TO DETAILED LONGITUDINAL
            write_out(Data, output_name="Hilmo", test=test)


def Hilmo_diagnosis_preparation(file_path:str, file_sep=";", test=False):
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
    Data = read_in(file_path, file_sep, dtype=dtypes, test=test)

    # keep only the main ICD diagnosis code and 3 extra ones 
    Data = Data.loc[ (Data.KENTTA.isin(["PDGO","SDGO"])) & (Data.N<=3)]
    Data = Data.reset_index(drop=True)

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



def Hilmo_operations_preparation(file_path:str, file_sep=";", test=False):
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
    Data = read_in(file_path, file_sep, dtype=dtypes, test=test)

    # keep only the main ICD diagnosis code and 3 extra ones 
    Data = Data.loc[Data.N<=3]
    Data = Data.reset_index(drop=True)

    # rename columns
    Data.rename( 
        columns = {
        "N":"CATEGORY",
        "TOIMP":"CODE1"
        },
        inplace=True )

    # keep only columns of interest
    Data = Data[ ["HILMO_ID","CATEGORY","CODE1"] ]

    return Data




def Hilmo_heart_preparation(file_path:str, file_sep=";", test=False):
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

    keys  = ["HILMO_ID"] + ["TMPC"+str(n) for n in range(1,12)] + ["TMPTYP"+str(n) for n in range(1,4)]
    values = [str for n in range(1,15)]
    dtypes = dict(zip(keys, values))

    # fetch Data
    Data = read_in(file_path, file_sep, dtype=dtypes, test=test)

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
        new_names = [CATEGORY_DICTIONARY.get(name, name) if s == name else s for s in new_names]

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
    Data = Data.reset_index(drop=True)
    Data = Data[ ["HILMO_ID","CATEGORY","CODE1"] ]

    return Data


def AvoHilmo_codes_preparation(file_path:str, source:str, file_sep=";", test=False):
    """Process AvoHilmo diagnosis codes.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        source (str): descriptor of the file containing the codes
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        test (bool, optional): Indicates whether the function is being called for testing purposes. Defaults to False.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the provided file_sep is not a valid separator.
    """

    CATEGORY_DICTIONARY = {
        "icd10":	["ICD10","ICD"],
        "icpc2":	["ICPC2","ICP"],
        "oral":		["TOIMENPIDE","MOP"],
        "oper":		["TOIMENPIDE","OP"]
    }

    # check thta source is correct
    assert source in CATEGORY_DICTIONARY.keys(), f"ERROR: {source} needs to be one of following: icd10, icpc2, oral, oper"

    source_col_name = CATEGORY_DICTIONARY[source][0]
    category_prefix = CATEGORY_DICTIONARY[source][1]

    dtypes = {
        "TNRO": str,
        "AVOHILMO_ID": int,
        "JARJESTYS": int,
        source_col_name: str
    }

    # fetch Data
    Data = read_in(file_path, file_sep, dtype=dtypes, test=test)	
    Data.rename( columns = {source_col_name:"CODE1"}, inplace=True )

    # keep only the main ICD diagnosis code and 3 extra ones  
    Data = Data.loc[ Data.JARJESTYS <= 3 ]
    Data = Data.reset_index(drop=True)

    # define the category column 
    Data["CATEGORY"] = np.NaN
    Data.loc[Data.CODE1.notna(),"CATEGORY"] = category_prefix + Data.loc[Data.CODE1.notna(),"JARJESTYS"].astype("string")

    # filter data
    Data = Data.loc[ Data.CODE1.notna() & Data.CATEGORY.notna() ]
    Data = Data.reset_index(drop=True)

    # remove ICD code dots
    Data["CODE1"] = Data["CODE1"].str.replace(".","",regex=False)

    return Data


def AvoHilmo_processing(file_path:str, DOB_map, extra_to_merge, source, year, file_sep=";", test=False):
    """Process AvoHilmo general file.

    This function reads and processes an AvoHilmo file located at the specified file_path.  
    also reads an extra AvoHilmo dataset about diagnosis/operations in order to add CODE1 and CATEGORY columns. 
    The processed data can be read/saved in a test setting if specified.

    Args:
        file_path (str): The path to the AvoHilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
        extra_to_merge (pd.dataframe): dataframe to map CODE1 and CATEGORY inside the AvoHilmo file
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
    "AVOHILMO_ID": int,
    "KAYNTI_ALKOI":str,
    "KAYNTI_YHTEYSTAPA":str,
    "KAYNTI_PALVELUMUOTO":str,
    "KAYNTI_AMMATTI":str,
    }

    # fetch Data
    chunksize = 10 ** 6
    with read_in_chunks(file_path, file_sep, dtype=dtypes, test=test) as reader:
        for Data in reader:

            # add date of birth/death
            Data = Data.merge(DOB_map,left_on = "TNRO",right_on = "FINREGISTRYID")
            # format date columns 
            Data["EVENT_DATE"] 		= pd.to_datetime( Data["KAYNTI_ALKOI"].str.slice(stop=10), format="%d.%m.%Y",errors="coerce" )

            # check if event is after death
            Data.loc[Data.EVENT_DATE > Data.DEATH_DATE,"EVENT_DATE"] = Data.DEATH_DATE

            #-------------------------------------------
            # define columns for detailed longitudinal
            Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
            Data["EVENT_YEAR"]		= Data.EVENT_DATE.dt.year
            Data["ICDVER"]			= 10
            Data["SOURCE"]			= "PRIM_OUT"
            Data["INDEX"]			= Data.AVOHILMO_ID
            Data["CODE2"]			= np.NaN
            Data["CODE3"]			= np.NaN
            Data["CODE4"]			= np.NaN
            Data["CODE8"]			= np.NaN
            Data["CODE9"]			= np.NaN

            # rename columns
            Data.rename( 
                columns = {
                "KAYNTI_YHTEYSTAPA":"CODE5",
                "KAYNTI_PALVELUMUOTO":"CODE6",
                "KAYNTI_AMMATTI":"CODE7",
                "EVENT_DATE":"EVENT_DAY"
                },
                inplace=True )

            #-------------------------------------------

            # merge CODE1 and CATEGORY from extra file
            Data = Data.merge(extra_to_merge, on = "AVOHILMO_ID", how="left")

            # remove missing CODE1
            Data = Data.dropna(subset=["CODE1"])
            Data = Data.reset_index(drop=True)

            # special character split
            Data = combination_codes_split(Data)
            # no PALTU mapping required

            #-------------------------------------------
            # QUALITY CONTROL:

            # check that EVENT_AGE is in predefined range 
            Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110)]
            Data = Data.reset_index(drop=True)
            # check that EVENT_AGE is not missing
            Data = Data.dropna(subset=["EVENT_AGE"])
            Data = Data.reset_index(drop=True)
            # if -1 in a CODE column replace with missing
            Data = fix_missing_value(Data)

            # select desired columns 
            Data = Data[ COLUMNS_2_KEEP ]

            # WRITE TO DETAILED LONGITUDINAL
            output_name = "Avohilmo_" + source + "_" + year
            write_out(Data, output_name=output_name, test=test) 


def DeathRegistry_processing(file_path:str, DOB_map, file_sep=";", test=False):
    """Process the information from death registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
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
    Data = read_in(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test)
    # add date of birth
    Data = Data.merge(DOB_map, left_on = "TNRO",right_on = "FINREGISTRYID")
    # format date columns (birth date)
    Data["EVENT_DATE"]		= pd.to_datetime( Data.KPV.str.slice(stop=10), format="%d.%m.%Y" )

    # define columns for detailed longitudinal
    Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
    Data["EVENT_YEAR"] 		= Data.EVENT_DATE.dt.year	
    Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
    Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
    Data["SOURCE"] 			= "DEATH"
    Data["CODE2"]			= np.NaN
    Data["CODE3"]			= np.NaN
    Data["CODE4"]			= np.NaN
    Data["CODE5"]			= np.NaN
    Data["CODE6"]			= np.NaN
    Data["CODE7"]			= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

    # rename columns
    Data.rename( 
        columns = {
        "EVENT_DATE":"EVENT_DAY"
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
        new_names = [CATEGORY_DICTIONARY.get(name, name) if s == name else s for s in new_names]

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
    Data = Data.dropna(subset=["CODE1"])
    Data = Data.reset_index(drop=True)

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110) ]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)

    # NOT performing code check in this registry

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="Death", test=test)



def CancerRegistry_processing(file_path:str, DOB_map, file_sep=";", test=False):
    """Process the information from cancer registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
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
    Data = read_in(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test)
    # add date of birth
    Data = Data.merge(DOB_map, on = "FINREGISTRYID")
    # format date columns (diagnosis date)
    Data["EVENT_DATE"]		= pd.to_datetime( Data.dg_date, format="%Y-%m-%d", errors="coerce" )

    #-------------------------------------------
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)	
    Data["EVENT_YEAR"] 		= Data.EVENT_DATE.dt.year	
    Data["ICDVER"] 			= "O3"
    Data["INDEX"] 			= np.arange(Data.shape[0] ) + 1
    Data["SOURCE"] 			= "CANC"
    Data["CATEGORY"] 		= np.NaN
    Data["CODE4"]			= np.NaN
    Data["CODE5"]			= np.NaN
    Data["CODE6"]			= np.NaN
    Data["CODE7"]			= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

    # rename columns
    Data.rename( 
        columns = {
        "topo":"CODE1",
        "morpho":"CODE2",
        "beh":"CODE3",
        "EVENT_DATE":"EVENT_DAY"
        },
        inplace=True)

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110) ]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ]
    Data = Data.reset_index(drop=True)

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="Cancer", test=test)



def KelaReimbursement_PRE20_processing(file_path:str, DOB_map, file_sep=";", test=False):
    """Process the information from kela reimbursement registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
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
    Data = read_in(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test)

    # add date of birth
    Data = Data.merge(DOB_map, left_on = "HETU",right_on = "FINREGISTRYID")
    # format date columns (reimbursement date)
    Data["REIMB_START"]		= pd.to_datetime( Data.ALPV, format="%Y-%m-%d", errors="coerce")
    Data["REIMB_END"]		= pd.to_datetime( Data.LOPV, format="%Y-%m-%d", errors="coerce")

    #-------------------------------------------
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.REIMB_START - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)
    Data["EVENT_YEAR"] 		= Data.REIMB_START.dt.year
    Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
    Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
    Data["SOURCE"] 			= "REIMB"
    Data["CATEGORY"] 		= np.NaN
    Data["CODE3"]			= np.NaN
    Data["CODE4"]			= np.NaN
    Data["CODE5"]			= np.NaN
    Data["CODE6"]			= np.NaN
    Data["CODE7"]			= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN    

    #rename columns
    Data.rename(
        columns = {
        "SK1":"CODE1",
        "DIAG":"CODE2",
        "REIMB_START":"EVENT_DAY"
        }, 
        inplace = True )

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110) ]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ]
    Data = Data.reset_index(drop=True)

    # remove ICD code dots
    Data["CODE2"] = Data["CODE2"].str.replace(".", "", regex=False)

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="KelaReimbursement", test=test)


def KelaReimbursement_20_21_processing(file_path:str, DOB_map, file_sep=";", test=False):
    """Process the information from kela reimbursement registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
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
    "korvausoikeus_alpv": str,
    "korvausoikeus_lopv": str,
    "KORVAUSOIKEUS_KOODI":  str,
    "DIAGNOOSI_KOODI": str
    }

    # fetch Data (manually)
    if test==False:
        Data = pd.read_csv(file_path, sep=file_sep, encoding='latin-1')
    else:
        Data = pd.read_csv(file_path, sep=file_sep, encoding='latin-1',nrows=5000)    
    # NB: fix header error <U+FEFF>HETU 
    Data.columns = ['HETU'] + Data.columns[1:].tolist()
    Data = Data[ dtypes.keys() ]


    # add date of birth
    Data = Data.merge(DOB_map, left_on = "HETU",right_on = "FINREGISTRYID")
    # format date columns (reimbursement date)
    Data["REIMB_START"]		= pd.to_datetime( Data.korvausoikeus_alpv, format="%Y-%m-%d", errors="coerce")
    Data["REIMB_END"]		= pd.to_datetime( Data.korvausoikeus_lopv, format="%Y-%m-%d", errors="coerce")

    #-------------------------------------------
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.REIMB_START - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)
    Data["EVENT_YEAR"] 		= Data.REIMB_START.dt.year
    Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
    Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
    Data["SOURCE"] 			= "REIMB"
    Data["CATEGORY"] 		= np.NaN
    Data["CODE3"]			= np.NaN
    Data["CODE4"]			= np.NaN
    Data["CODE5"]			= np.NaN
    Data["CODE6"]			= np.NaN
    Data["CODE7"]			= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

    #rename columns
    Data.rename(
        columns = {
        "KORVAUSOIKEUS_KOODI":"CODE1",
        "DIAGNOOSI_KOODI":"CODE2",
        "REIMB_START":"EVENT_DAY"
        }, 
        inplace = True )

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110) ]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ]
    Data = Data.reset_index(drop=True)

    # remove ICD code dots
    Data["CODE2"] = Data["CODE2"].str.replace(".", "", regex=False)

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]	

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="KelaReimbursement", test=test)	


def KelaPurchase_PRE20_processing(file_path:str, DOB_map, file_sep=";", test=False):
    """Process the information from kela purchases registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
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
    Data = read_in(file_path=file_path, file_sep=file_sep, dtype=dtypes, test=test)
    # add date of birth
    Data = Data.merge(DOB_map,left_on = "HETU",right_on = "FINREGISTRYID")
    # format date columns (purchase date)
    Data["EVENT_DATE"] 		= pd.to_datetime( Data.OSTOPV, format="%Y-%m-%d", errors="coerce") 	

    #-------------------------------------------	
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)
    Data["EVENT_YEAR"] 		= Data.EVENT_DATE.dt.year
    Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
    Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
    Data["SOURCE"] 			= "PURCH"
    Data["CATEGORY"] 		= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

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
        "EVENT_DATE":"EVENT_DAY"
        },
        inplace = True )

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110) ]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ]
    Data = Data.reset_index(drop=True)

    # remove dot in VNRO code
    Data["CODE3"] = Data.CODE3.astype("string").str.split(".",expand=True)[0]
    # complete VNR code if shorter than 6 digits
    MISSING_DIGITS = 6 - Data.CODE3.str.len()
    ZERO = pd.Series( ["0"] * Data.shape[0] ).astype("string")
    Data["CODE3"] = Data.CODE3 + ZERO*MISSING_DIGITS

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="KelaPurchase", test=test)   


def KelaPurchase_20_21_processing(file_path:str, DOB_map, file_sep=";", test=False):
    """Process the information from kela purchases registry.

    This function reads and processes file located at the specified file_path. 
    information about birth and death dates is provided via DOB_map. 
    The processed data can be read/saved in a test setting if specified.
    If not in testing setting the processed dataframe will be appended to the detailed longitudinal file.

    Args:
        file_path (str): The path to the Hilmo file.
        file_sep (str, optional): The separator used in the file. Defaults to ";".
        DOB_map (pd.dataframe): dataframe mapping DOB codes to their corresponding dates
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
    "ostopv": str,
    "ATC":  str,
    "SAIR": str,
    "VNRO": str,
    "PLKM": str,
    "korv_eur": str,
    "kakorv_eur": str,
    "LAJI": str,
    }

    # fetch Data (manually)
    if test==False:
        Data = pd.read_csv(file_path, sep=file_sep, encoding='latin-1')
    else:
        Data = pd.read_csv(file_path, sep=file_sep, encoding='latin-1',nrows=5000)   
    # NB: fix header error <U+FEFF>HETU 
    Data.columns = ['HETU'] + Data.columns[1:].tolist()
    Data = Data[ dtypes.keys() ]
    # add date of birth
    Data = Data.merge(DOB_map,left_on = "HETU",right_on = "FINREGISTRYID")
    # format date columns (purchase date)
    Data["EVENT_DATE"] 		= pd.to_datetime( Data.ostopv, format="%Y-%m-%d", errors="coerce") 	

    #-------------------------------------------	
    # define columns for detailed longitudinal

    Data["EVENT_AGE"] 		= round( (Data.EVENT_DATE - Data.BIRTH_DATE).dt.days/DAYS_TO_YEARS, 2)
    Data["EVENT_YEAR"] 		= Data.EVENT_DATE.dt.year
    Data["ICDVER"] 			= 8 + (Data.EVENT_YEAR>1986).astype(int) + (Data.EVENT_YEAR>1995).astype(int) 
    Data["INDEX"] 			= np.arange(Data.shape[0]) + 1
    Data["SOURCE"] 			= "PURCH"
    Data["CATEGORY"] 		= np.NaN
    Data["CODE8"]			= np.NaN
    Data["CODE9"]			= np.NaN

    #rename columns
    Data.rename(
        columns = {
        "ATC":"CODE1",
        "SAIR":"CODE2",
        "VNRO":"CODE3",
        "PLKM":"CODE4",
        "korv_eur":"CODE5",
        "kakorv_eur":"CODE6",
        "LAJI":"CODE7",
        "EVENT_DATE":"EVENT_DAY"
        },
        inplace = True )

    #-------------------------------------------
    # QUALITY CONTROL:

    # check that EVENT_AGE is in predefined range 
    Data = Data.loc[ (Data.EVENT_AGE>0) & (Data.EVENT_AGE<=110) ]
    Data = Data.reset_index(drop=True)
    # check that EVENT_AGE is not missing
    Data = Data.dropna(subset=["EVENT_AGE"])
    Data = Data.reset_index(drop=True)
    # check that CODE1 and 2 are not missing
    Data = Data.loc[ Data.CODE1.notna() | Data.CODE2.notna()  ]
    Data = Data.reset_index(drop=True)

    # remove dot in VNRO code
    Data["CODE3"] = Data.CODE3.astype("string").str.split(".",expand=True)[0]
    # complete VNR code if shorter than 6 digits
    MISSING_DIGITS = 6 - Data.CODE3.str.len()
    ZERO = pd.Series( ["0"] * Data.shape[0] ).astype("string")
    Data["CODE3"] = Data.CODE3 + ZERO*MISSING_DIGITS

    # select desired columns 
    Data = Data[ COLUMNS_2_KEEP ]

    # WRITE TO DETAILED LONGITUDINAL
    write_out(Data, output_name="KelaPurchase", test=test)      
