
# LIBRARIES

import re
import pandas as pd
import numpy as np


#--------------------
# UTILITY FUNCTIONS

def SpecialCharacterSplit(data):

	#------------------
	# RULE: if CODE1 has a '*' then ?!

	#------------------
	# RULE: if CODE1 has a '+' then first part goes to CODE2 and second to CODE1

	data['IS_PLUS'] = data.CODE1.str.match('+')
	data_tosplit 	= data_tosplit.loc[data_tosplit['IS_PLUS'] == True]
	part1, part2 	= data_tosplit['CODE1'].str.split('\\+')

	data.loc[data.IS_PLUS == True]['CODE2'] = part1
	data.loc[data.IS_PLUS == True]['CODE1'] = part2

	#------------------
	# RULE: if CODE1 has a '#' then first part goes to CODE1 and second to CODE3

	data['IS_HAST']	= data.CODE1.str.match('+')
	data_tosplit 	= data_tosplit.loc[data_tosplit['IS_HAST'] == True]
	part1, part2 	= data_tosplit['CODE1'].str.split('\\+')

	data.loc[data['IS_HAST'] == True]['CODE1'] = part1
	data.loc[data['IS_HAST'] == True]['CODE3'] = part2

	#------------------
	# RULE: if CODE1 has a '&' then first part goes to CODE1 and second to CODE2

	data['IS_AND'] 	= data.CODE1.str.match('+')
	data_tosplit 	= data_tosplit.loc[data_tosplit['IS_AND'] == True]
	part1, part2 	= data_tosplit['CODE1'].str.split('\\+')

	data.loc[data['IS_AND'] == True]['CODE1'] = part1
	data.loc[data['IS_AND'] == True ]['CODE2'] = part2

	return data	


#-------------------
# UTILITY EXTRA

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
	"CODE7",
	"CODE8", 
	"CODE9"]




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



def Hilmo_PRE95_preparation(hilmo_pre95, DOB_map):

	# add date of birth
	NewData = hilmo_pre95.merge(DOB_map,left_on = 'TNRO',right_on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['EVENT_AGE'] 	= round( (NewData.TULOPVM - NewData.SYNTPVM).days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YRMNTH']	= NewData['TULOPVM'].to_string()[:7]
	NewData['INDEX'] 		= np.arange( 1, NewData.shape[0]+1 )
	NewData['SOURCE'] 		= 'INPAT'
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= NewData.LAHTOPVM - NewData.TULOPVM
	NewData['CATEGORY']		= ...

	#rename columns
	NewData.rename( 
		columns = {
		'TULOPVM':'PVM'
		}, 
		inplace=True )

	# select desired columns 
	SubsetData = NewData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 
	# check negative hospital days ? -> put NA 
	...

	# remove duplicates ?
	...

	return CodeCheck



def Hilmo_POST95_Preparation(file_path:str,file_sep:str):

	# NB: this part of Hilmo is using ICD10 codes
	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	#rename columns
	SubsetData.rename( columns = {'TULOPVM':'PVM'}, inplace=True )

	# select desired columns 
	SubsetData = OriginalData[ COLUMNS_2_KEEP ]

	# remove wrong codes
	wrong_codes = ['H','M','N','Z6','ZH','ZZ']
	FinalData = SubsetData.loc[SubsetData.PALA in wrong_codes]

	return FinalData


	

def HilmoOperations_Preprocessing(file_path:str,file_sep:str):

	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# select desired columns 
	SubsetData = OriginalData[ HilmoOperations_col2keep ]

	# add columns
	SubsetData['INDEX'] 		= np.arange( 1, SubsetData.shape[0]+1 )
	SubsetData['PTMPK1']		= NP.NaN
	SubsetData['PTMPK2']		= NP.NaN
	SubsetData['PTMPK3']		= NP.NaN
	SubsetData['MTMP1K1']		= NP.NaN
	SubsetData['MTMP2K1']		= NP.NaN
	SubsetData['SOURCE'] 		= 'OPER_IN'
	SubsetData['EVENT_YRMNTH']	= SubsetData['TULOPVM'].to_string()[:7]

	#rename columns
	SubsetData.rename( columns = {'TULOPVM':'PVM'}, inplace=True )

	# add category depending on what ? 
	SubsetData['CATEGORY'] 		=  ...

	# missing values
	SubsetData.loc[SubsetData==''] = np.NaN


def HilmoPreprocessing():

	# download different datasets:
	print('downloading Hilmo and extra datasets')
	codes  	= ... #txt file to find
	diag 	= ...
	toimp 	= ...
	syp		= ...
	ulksyy 	= ... #not found (look before hilmo in original script)
	Hilmo 	= ... #is the Hilmo_69_95 file

	# select desired columns
	diag_subset  = diag[ ... ] 
	toimp_subset = toimp[['HILMO_ID';'TNRO';'N';'TOIMP']]
	syp_merged	 = syp[ ... ]
	# is HILMO_ID the same as TID for finngen?

	# merge dataframes
	diag_merged = diag.merge(Hilmo, on='TID')
	diag_merged.rename(columns = {'TNRO_x':'TNRO_orig'},inplace=True).drop('TNRO_y',inplace=True)

	toimp_merged = toimp_subset.merge(Hilmo, on='TID')
	toimp_merged.rename(columns = {'TNRO_x':'TNRO_orig'},inplace=True).drop('TNRO_y',inplace=True)

	syp_merged = syp_subset.merge(Hilmo, on='TID')
	syp_merged.rename(columns = {'TNRO_x':'TNRO_orig'},inplace=True).drop('TNRO_y',inplace=True)

	# rename columns
	diag_merged.rename( columns = {'SDGNRO':'CATEGORY','KOODI1':'CODE1','KOODI2':'CODE2'},inplace=True )
	toimp_merged.rename( columns = {'ICPC2':'CODE1'},inplace=True )
	syp_merged.rename( columns = {'TOIMENPIDE':'CODE1'},inplace=True )

	#----------------------
	# define code columns

	# DIAGNOSIS
	diag_merged['CODE34'] = np.NaN
	if diag_merged.CODE1 in codes: 
		diag_merged['CODE3'] = diag_merged['CODE2']
		diag_merged['CODE2'] = np.NaN
	else:
		diag_merged['CODE3'] = np.NaN

	# TOIMP
	toimp['CATEGORY'] = ...
	toimp['CODE2'] = np.NaN
	toimp['CODE3'] = np.NaN
	toimp['CODE4'] = np.NaN

	# HEART
	# the following code will reshape the dataframe from wide to long
	# if there was a value under one of the category columns this will be transferred under the column CODE1
	# NB: the category columns are going to be renamed beforehand as desired   

	# rename categories
	column_names 	= syp_merged.columns
	new_names		= [s.replace('TMPTYP','HPO') for s in column_names]
	new_names 		= [s.replace('TMPC','HPN') for s in new_names]
	syp_merged.columns = new_names

	# perform the reshape
	VAR_FOR_RESHAPE = syp_merged.columns[ syp_merged.columns in [set(new_names)^set(column_names)] ]
	VAR_FOR_RESHAPE = VAR_FOR_RESHAPE.insert('TID')

	syp_reshaped = pd.melt(syp_merged[ TO_RESHAPE ],
		id_vars 	= 'TID',
		value_vars 	= VAR_FOR_RESHAPE,
		var_name 	= 'CATEGORY',
		value_name	= 'CODE1')

	#remove patient row if category is missing
	...

	#create the final dataset
	VAR_NOT_FOR_RESHAPE = syp_merged.columns[ syp_merged.columns != VAR_FOR_RESHAPE.remove('TID') ]
	syp_final = syp_reshaped.merge(syp_merged[ VAR_NOT_FOR_RESHAPE ], on = 'TID')

	# add other (empty) code columns 
	syp_final['CODE2'] = np.NaN
	syp_final['CODE3'] = np.NaN
	syp_final['CODE4'] = np.NaN



	#-------------------------------
	# creating the final dataframe
	diag_final  = diag_merged[ ['TID','TNRO_orig','PALTU','PALA','EA','TUPVA','LPVM','CODE1','CODE2','CODE3','CODE4','CATEGORY','YHTEYSTAPA','KIIREELLISYYS']]
	toimp_final = toimp_merged[['TID','TNRO_orig','PALTU','PALA','EA','TUPVA','LPVM','CODE1','CODE2','CODE3','CODE4','CATEGORY','YHTEYSTAPA','KIIREELLISYYS']]
	syp_final 	= syp_merged[  ['TID','TNRO_orig','PALTU','PALA','EA','TUPVA','LPVM','CODE1','CODE2','CODE3','CODE4','CATEGORY','YHTEYSTAPA','KIIREELLISYYS']]

	FinalData = pd.concat( [diag_final, toimp_final, syp_final] )

	# final touches
	FinalData['ICDVER'] = 10
	FinalData['INDEX']  = Hilmo.TID + '_ICD10'
	FinalData.rename(columns = {'TNRO_orig':'TNRO','TULOPVM':'PVM'}, inplace=True)
	FinalHilmo = FinalData.loc[ !( FinalData.CODE1.isna() & FinalData.CODE2.isna() )] 

	return FinalHilmo	

def AvoHilmoPreprocessing():

	# download different datasets:
	print('downloading AvoHilmo and extra datasets')
	icd10 	 = ...
	icpc2 	 = ...
	suu		 = ...
	toimp 	 = ...
	AvoHilmo = ... #not found

	# select desired columns 
	icd10_subset 	= icd10[ ['TID','TNRO','JARJESTYS','ICD10'] ]
	icpc2_subset 	= icpc2[ ['TID','TNRO','JARJESTYS','ICPC2'] ]
	suu_subset 		= suu[   ['TID','TNRO','JARJESTYS','TOIMENPIDE'] ]
	toimp_subset 	= toimp[ ['TID','TNRO','JARJESTYS','TOIMENPIDE'] ]
	AvoHilmo_subset = AvoHilmo[ AvoHilmo_col2keep ]

	# define the category column 
	icd10_subset['CATEGORY'] = np.NaN
	to_update = icd10_subset.iloc[ !icd10_subset.ICD10.isna() ]
	icd10_subset[to_update,'CATEGORY'] = icd10_subset.ICD + icd10_subset.JARJESTYS 

	icpc2_subset['CATEGORY'] = np.NaN
	to_update = icpc2_subset.iloc[ !icpc2_subset.ICPC2.isna() ]
	icpc2_subset[to_update,'CATEGORY'] = icpc2_subset.ICP + icpc2_subset.JARJESTYS 

	suu_subset['CATEGORY'] = np.NaN
	to_update = suu_subset.iloc[ !suu_subset.TOIMENPIDE.isna() ]
	suu_subset[to_update,'CATEGORY'] = suu_subset.OP + suu_subset.JARJESTYS

	toimp_subset['CATEGORY'] = np.NaN
	to_update = toimp_subset.iloc[ !toimp_subset.TOIMENPIDE.isna() ]
	toimp_subset[to_update,'CATEGORY'] = toimp_subset.OP + toimp_subset.JARJESTYS

	# rename columns
	icd10_subset.rename( columns = {'ICD10':'CODE1'},inplace=True )
	icpc2_subset.rename( columns = {'ICPC2':'CODE1'},inplace=True )
	suu_subset.rename(   columns = {'TOIMENPIDE':'CODE1'},inplace=True )
	toimp_subset.rename( columns = {'TOIMENPIDE':'CODE1'},inplace=True )

	# merge dataframes
	icd10_merged = icd10_subset.merge(AvoHilmo, on='TID')
	icd10_merged.rename(columns = {'TNRO_x':'TNRO_orig'},inplace=True).drop('TNRO_y',inplace=True)

	icpc2_merged = icpc2_subset.merge(AvoHilmo, on='TID')
	icpc2_merged.rename(columns = {'TNRO_x':'TNRO_orig'},inplace=True).drop('TNRO_y',inplace=True)

	suu_merged = suu_subset.merge(AvoHilmo, on='TID')
	suu_merged.rename(columns = {'TNRO_x':'TNRO_orig'},inplace=True).drop('TNRO_y',inplace=True)

	toimp_merged = toimp_subset.merge(AvoHilmo, on='TID')
	toimp_merged.rename(columns = {'TNRO_x':'TNRO_orig'},inplace=True).drop('TNRO_y',inplace=True)

	# filtering data
	icd10_final = icd10_merged.loc[ !( icd10_merged.CODE1.isna() & icd10_merged.CATEGORY.isna() & icd10_merged.JARJESTYS.isna() )] 
	icpc2_final = icpc2_merged.loc[ !( icpc2_merged.CODE1.isna() & icpc2_merged.CATEGORY.isna() & icpc2_merged.JARJESTYS.isna() )] 
	suu_final   = suu_merged.loc[   !( suu_merged.CODE1.isna() & suu_merged.CATEGORY.isna() & suu_merged.JARJESTYS.isna() )] 
	toimp_final = toimp_merged.loc[ !( toimp_merged.CODE1.isna() & toimp_merged.CATEGORY.isna() & toimp_merged.JARJESTYS.isna() )] 

	# creating the final dataframe
	FinalData = pd.concat([ icd10_final, icpc2_final, suu_final, toimp_final ])

	return FinalData



def AvoHilmoPreprocessing_pt2(AvoHilmo_pt1_output):
	AvoHilmo = AvoHilmo_pt1_output

	# rename columns
	AvoHilmo.rename( 
		columns = {
		'TID':'INDEX',
		'KAYNTI_YHTEYSTAPA':'CODE5',
		'KAYNTI_PALVELUMUOTO':'CODE6',
		'KAYNTI_AMMATTI'='CODE7'
		},
		inplace=True)

	# add columns
	AvoHilmo['PVM']			= FinalData.KAYNTI_ALKOI.to_datetime('%d.%m.%Y')
	AvoHilmo['EVENT_YEAR'] 	= FinalData.KAYNTI_ALKOI.str.slice(6,10)
	AvoHilmo['SYNTPVM']		= htun2date(FinalData.HETU)
	#recheck event age code ... doiesn'0t make sense
	AvoHilmo['EVENT_AGE']	= ((FinalData.KAYNTI_ALKOI.to_datetime('%d.%m.%Y') - FinalData.KAYNTI_ALKOI.to_datetime('%Y-%m-%d'))/365.24).round(2)
	AvoHilmo['ICDVER'] 		= 10
	AvoHilmo['SOURCE']		= 'PRIM_OUT'
	AvoHilmo['EVENT_YRMNTH']= FinalData.KAYNTI_ALKOI.str.slice(2,6)
	AvoHilmo['CODE2']		= np.NaN
	AvoHilmo['CODE3']		= np.NaN
	AvoHilmo['CODE4']		= np.NaN
	AvoHilmo['CODE8']		= np.NaN
	AvoHilmo['CODE9']		= np.NaN


	# subset columns
	AvoHilmo_subset = AvoHilmo[ AvoHilmo_pt2_col2keep ]

	# FILTERING

	AvoHilmo_subset.loc[AvoHilmo_subset==''] = np.NaN
	# remove missing event age
	AvoHilmo_agecheck = AvoHilmo_subset.loc[ !AvoHilmo_subset.EVENT_AGE.isna() ]
	# remove missing codes
	AvoHilmo_codecheck = AvoHilmo_agecheck.loc[ !( AvoHilmo_agecheck.CODE1.isna() & AvoHilmo_agecheck.CODE2.isna() )] 
	# remove rows if patient ID is in denied list
	ID_DENIED = ... #to download file
	AvoHilmo_idcheck = AvoHilmo_codecheck.loc[ AvoHilmo_codecheck.FINREGISTRYID not in ID_DENIED]

	#remove duplicates?
	...

	return AvoHilmo_idcheck

def DeathRegistry_preparation(file_path:str, file_sep:str, DOB_map):
	
	# fetch data
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['dg_date']		= pd.to_datetime( data['dg_date'], format='%Y-%m-%d' )
	NewData['EVENT_AGE'] 	= round( (NewData.TULOPVM - NewData.SYNTPVM).days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YEAR'] 	= NewData.dg_date.year	
	NewData['EVENT_YRMNTH']	= NewData['dg_date'].to_string()[:7]
	NewData['INDEX'] 		= np.arange( 1, NewData.shape[0]+1 )
	NewData['SOURCE'] 		= 'DEATH'
	NewData['CODE2']		= np.NaN
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= np.NaN
	NewData['CATEGORY'] 	=  ...
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 

	# rename columns
	SubsetData.rename( 
		columns = {
		...
		},
		inplace=True)

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# remove duplicates ?
	...

	return CodeCheck



def CancerRegistry_preparation(file_path:str, file_sep:str, DOB_map):
	
	# fetch data
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['dg_date']			= pd.to_datetime( data['dg_date'], format='%Y-%m-%d' )
	NewData['EVENT_AGE'] 		= round( (NewData.TULOPVM - NewData.SYNTPVM).days/DAYS_TO_YEARS, 2)	
	NewData['EVENT_YEAR'] 		= NewData.dg_date.year	
	NewData['EVENT_YRMNTH']		= NewData.dg_date[:7]
	NewData['ICDVER'] 			= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	NewData['MY_CANC_COD_TOPO'] = np.NaN
	NewData['MY_CANC_COD_AGE'] 	= np.NaN
	NewData['MY_CANC_COD_YEAR'] = np.NaN
	NewData['INDEX'] 			= np.arange( 1, NewData.shape[0]+1 )
	NewData['SOURCE'] 			= 'CANC'
	NewData['CATEGORY'] 		= np.NaN  # maybe is 'O3' but in the code is saying to put this in ICDVER .. 
	NewData['CODE4']			= np.NaN

	# rename columns
	SubsetData.rename( 
		columns = {
		'topo':'CODE1',
		'morpho':'CODE2',
		'beh':'CODE3',
		'dg_date':'PVM'
		},
		inplace=True)

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# remove duplicates ?
	...

	return CodeCheck



def KelaReimbursement_preparation(file_path:str, file_sep:str, DOB_map):

	# fetch data
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = data.merge(DOB_map, on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['LAAKEKORVPVM']	= pd.to_datetime( data['ALPV'], format='%Y-%m-%d' )
	NewData['EVENT_AGE'] 	= round( (NewData.LAAKEKORVPVM - FinalData.SYNTPVM).days/DAYS_TO_YEARS, 2)
	NewData['EVENT_YEAR'] 	= NewData.LAAKEKORVPVM.year
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	NewData['INDEX'] 		= np.arange( 1, NewData.shape[0]+1 )
	NewData['SOURCE'] 		= 'REIMB'
	NewData['CATEGORY'] 	= np.NaN
	NewData['CODE3']		= np.NaN
	NewData['CODE4']		= np.NaN
	NewData['EVENT_YRMNTH']	= NewData.LAAKEKORVPVM[:7]
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
	SubsetData = OriginalData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# remove duplicates ?
	...

	# remove ICD code dots
	CodeCheck['CODE2'] = CodeCheck['CODE2'].replace({".", ""})
	return CodeCheck



def KelaPurchase_preparation(file_path:str, file_sep:str,DOB_map):

	# create aggregated dataset
	data = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')
	# add date of birth
	NewData = data.merge(DOB_map,left_on = 'HETU',right_on = 'FINREGISTRYID')
	NewData.rename( 'DOB(DD-MM-YYYY-format)':'SYNTPVM', inplace = True )

	# define new columns
	NewData['LAAKEOSTPVM'] 	= pd.to_datetime( data['OSTOPV'], format='%Y-%m-%d' ) 
	NewData['EVENT_AGE'] 	= round( (NewData.LAAKEOSTPVM - NewData.SYNTPVM).days/DAYS_TO_YEARS, 2)
	NewData['EVENT_YEAR'] 	= NewData.LAAKEOSTPVM.year
	NewData['EVENT_YRMNTH']	= NewData.LAAKEOSTPVM[:7]
	NewData['ICDVER'] 		= 8 + (NewData.EVENT_YEAR>1986).astype(int) + (NewData.EVENT_YEAR>1995).astype(int) 
	NewData['INDEX'] 		= np.arange( 1, NewData.shape[0]+1 )
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


	# select desired columns 
	SubsetData = NewData[ COLUMNS_2_KEEP ]

	# remove missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	AgeCheck 	= SubsetData.loc[ !SubsetData.EVENT_AGE.isna() ]
	CodeCheck 	= AgeCheck.loc[ !( AgeCheck.CODE1.isna() & AgeCheck.CODE2.isna() )] 

	# remove duplicates ?
	...

	return CodeCheck
	

def CreateDetailedLongitudinal(hilmo_pre95,hilmo_pre95_operations,hilmo_post95_inpat,hilmo_post95_outpat,avohilmo,kela_purchases,kela_reimbursement,cancer,causeofdeath):

	# DATA SPLITS
	AvoHilmo_splitted 		= SpecialCharacterSplit(avohilmo)
	Hilmo_pre95_splitted 	= SpecialCharacterSplit(hilmo_pre95)
	Hilmo_post95_splitted 	= SpecialCharacterSplit(hilmo_post95)

	# FIX NOMESCO/INDEXES

	# JOIN DATA

	# select the following columns for everyone
	# but why here ?! and why again ?!
	# "FINNGENID","SOURCE", "EVENT_AGE", "PVM", "EVENT_YRMNTH", 
	# "CODE1", "CODE2", "CODE3", "CODE4", "CODE5", "CODE6", "CODE7", "CODE8", "CODE9", 
	# "ICDVER", "CATEGORY", "INDEX"


	# all missing values are setted to NA, no ""

	# CERATE FINAL DATASET
	JointData = pd.concat(
		AvoHilmo_splitted, 
		Hilmo_pre95_splitted,
		Hilmo_post95_splitted,
		avohilmo,
		kela_purchases,
		kela_reimbursement,
		cancer,
		causeofdeath
		)

	# FINALIZE
	FilteredData = JointData.loc[ !JointData.EVENT_AGE<0 & !JointData.EVENT_AGE>110]
	SortedData = FilteredData.sort_values( by = ['FINREGISTRYID','EVENT_AGE'])

	# age randomization
	# NOT IN FINREGISTRY - ONLY IN FINNGEN

	# add PALTU info
	palm = pd.read_csv("PALTU_mapping.csv",file_sep=',')
	SortedData['CODE7'] = SortedData.CODE7.to_numeric
	FinalData = SortedData.merge(palm, left_on="CODE7", right_on="PALTU")
	# correct missing PALTU
	registry_tocheck = ["INPAT", "OUTPAT", "OPER_IN", "OPER_OUT"]
	FinalData.loc[FinalData.CODE7 in registry_tocheck & FinalData.CODE7.isna()]['CODE7'] = 'Other Hospital' 

	return FinalData

