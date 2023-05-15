
# PREPROCESSING FOR DETAILED LONGITUDINAL

# this code is used to prepare some datasets for then construct the detailed longitudinal file_path
# therefore some of those registries are going to appear in multiple scripts 

# LIBRARIES

import os
import re
import string
from datetime import datetime
import pandas as pd
import numpy as np

#--------------------
# UTILITY FUNCTIONS

def htun2date(ht):
	pass()


#---------------------
# REGISTRY-SPECIFIC FUNCTIONS

def Hilmo_69_86_preparation(file_path:str, file_sep:str):

	#fetch data
	data = pd.read_csv(
		file_path,
		sep = file_sep, 
		encoding='latin-1'
		#dtype={"TUPVA": "string", "JOPVM": "string",}
		)

	data['ICDVER'] = 8
	data.rename( 
		columns = {
		'DG1':'PDGO',
		'DG2':'SDG10',
		'DG3':'SDG20',
		'DG4':'SDG30'
		'TULOPVM':'TULOPV',
		'LAHTOPVM':'LAHTOPV',
		'REKVV':'EVENT_YEAR'
		},
		inplace=True)
	data['VUOSI'] = data['TULOPV'].strftime('%Y-%m-%d').year
	return data

def Hilmo_87_93_preparation(file_path:str, file_sep:str):

	#fetch data
	data = pd.read_csv(
		file_path,
		sep = file_sep, 
		encoding='latin-1'
		#dtype={"TUPVA": "string", "JOPVM": "string",}
		)

	data['ICDVER'] = 9
	data.rename( 
		columns = {
		'PDG' :'PDGO',
		'SDG1':'SDG10',
		'SDG2':'SDG20',
		'SDG3':'SDG30',
		'TULOPVM':'TUPVA',
		'LAHTOPVM':'LPVM',
		'VUOSI':'EVENT_YEAR'
		},
		inplace=True)
	return data


def Hilmo_94_95_preparation(file_path:str, file_sep:str):

	#fetch data
	data = pd.read_csv(
		file_path,
		sep = file_sep, 
		encoding='latin-1'
		#dtype={"TUPVA": "string", "JOPVM": "string",}
		)

	data['ICDVER'] = 9
	data.rename( 
		columns = {
		'PDG' :'PDGO',
		'SDG1':'PDG1O',
		'SDG2':'SDG20',
		'TMPTYP1':'TPTYP1',
		'TMPTYP2':'TPTYP2',
		'TMPTYP3':'TPTYP3',
		'TULOPVM':'TUPVA',
		'LAHTOPVM':'LPVM',
		'VUOSI':'EVENT_YEAR'
		},
		inplace=True)
	return data



def HilmoPreprocessing(dataset_list:list):
	# combine Hilmo datasets
	AggregatedData = pd.concat(dataset_list)

	# date/age
	AggregatedData['SYNTPVM'] 		= htun2date(AggregatedData['HETU'])

	# remove events with missing date
	NewData = AggregatedData.loc[ np.isnan(AggregatedData.EVENT_AGE) ] 
	return NewData




def DeathRegistryPreprocessing(dataset_list:list):
	# combine Hilmo datasets
	AggregatedData = pd.concat(dataset_list)

	# date/age
	AggregatedData['KUOLPVM'] 		= ...
	AggregatedData['SYNTPVM'] 		= htun2date(AggregatedData['HETU'])
	formatted_date = datetime.strptime(AggregatedData['KUOLPVM'], '%Y-%m-%d')
	AggregatedData['EVENT_AGE'] 	= int(formatted_date).round(2)
	AggregatedData.rename(columns = {kvuosi:EVENT_YEAR},inplace=True)

	# ICD
	AggregatedData['ICDVER'] = 8 + (AggregatedData.EVENT_YEAR>1986).astype(int) + (AggregatedData.EVENT_YEAR>1995).astype(int) 
	return AggregatedData


def CancerRegistryPreprocessing(dataset_list:list):
	data = ...
	#join link file
	link_file = ...
	AggregatedData = pd.merge(data,link_file,on='gen_henk_id')

	# date/age
	AggregatedData['SYNTPVM'] 		= htun2date(AggregatedData['HETU'])
	formatted_date = datetime.strptime(AggregatedData['dg_date'], '%d.%m.%Y')
	AggregatedData['EVENT_AGE'] 	= int(formatted_date).round(2))
	AggregatedData['EVENT_YEAR'] 	= formatted_date.year

	# ICD
	AggregatedData['ICDVER'] = 8 + (AggregatedData.EVENT_YEAR>1986).astype(int) + (AggregatedData.EVENT_YEAR>1995).astype(int) 
	
	# other
	AggregatedData['MY_CANC_COD_TOPO'] 	= np.NaN
	AggregatedData['MY_CANC_COD_AGE'] 	= np.NaN
	AggregatedData['MY_CANC_COD_YEAR'] 	= np.NaN
	return ... 


def KelaPurchasePreprocessing(purchase_files:list, ):

	# select file related to year 2010
	if re.search('2010',purchase_files[17]) :
		FILE_INDEX_NUMBER = 17
	else:
  		print("An error has occured, file 17 is no more referring to the year 2010 .. time to update the code!")

	# 1994-2010
	Kela_pt1 = pd.concat(purchase_files[:FILE_INDEX_NUMBER])
	Kela_pt1.columns = Kela_pt1.columns.upper

	# 2011-2021
	Kela_pt2 = pd.concat(purchase_files[FILE_INDEX_NUMBER:])
	Kela_pt2.columns = Kela_pt2.columns.upper

	# concatenate
	AggregatedData = pd.concat(Kela_pt1,Kela_pt2)

	# select columns?

	# add figid

	# missing values
	AggregatedData.fillna('')

	# date/age
	AggregatedData['SYNTPVM'] 		= htun2date(AggregatedData['HETU'])
	AggregatedData['LAAKEOSTPVM'] 	= datetime.strptime( AggregatedData['OTPVM'].astype(char), '%d.%m.%Y')
	AggregatedData['EVENT_AGE'] 	= int(AggregatedData['LAAKEOSTPVM']).round(2)
	AggregatedData['EVENT_YEAR'] 	= AggregatedData['LAAKEOSTPVM'].year

	# remove events with missing date
	NewData = AggregatedData.loc[ np.isnan(AggregatedData.EVENT_AGE) ]  

	# ICD 
	NewData['ICDVER'] = 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	return ...
	
	

#--------------------
# MAIN  

if __name__ == '__main__':

	Hilmo_1 = Hilmo_69_86_preparation()
	Hilmo_2 = Hilmo_87_93_preparation()
	Hilmo_3 = Hilmo_94_95_preparation()
	processed_HILMO = HilmoPreprocessing()
	processed_death_registry = DeathRegistryPreprocessing()
	processed_cancer_registry = CancerRegistryPreprocessing()
	processed_kela_purchases = KelaPurchasePreprocessing()
