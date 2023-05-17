
# PREPROCESSING FOR DETAILED LONGITUDINAL

# second part of processing, check script_1.py for first part

# ------------------
# LIBRARIES

import pandas as pd
import numpy as np

#--------------------
# UTILITY FUNCTIONS

def htun2date(ht):
	pass()


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

KelaPurcahse_col2keep 		= ['FINREGISTRYID','EVENT_AGE','LAAKEOSTPVM','ATC_CODE','SAIR','VNRO','PLKM','ICDVER','KORV','KAKORV','LAJI']
KelaReimbursement_col2keep	= ['FINREGISTRYID','EVENT_AGE','LAAKEKORVPVM','KELA_DISEASE','ICD','ICD_VER']
CancerRegistry_col2keep		= ['FINREGISTRYID','EVENT_AGE','topo','morpho','beh','ICDVER','dg_date_']
CauseOfDeath_col2keep		= ['FINREGISTRYID','EVENT_AGE','ICDVER','KUOLPVM','tpks','vks','m1','m2','m3','m4']
Hilmo_69_95_col2keep 		= ['FINREGISTRYID','EVENT_AGE','PDGO','PDGE', 'SDG10','SDG1E','SDG2O','SDG2E','SDG3O','SDG3E', 'ATC_CODE1','ATC_CODE2','ATC_CODE3','ICD_VER','TULOPVM','LAHTOPVM','EDIA','PALA','PALTU','EA']
Hilmo_POST95_col2keep		= ['TID','TNRO','PALTU','PALA','EA','TUPVA','LPVM','YHTEYSTAPA','KIIREELLISYYS']
HilmoOperations_col2keep	= []
AvoHilmo_col2keep			= ['TID','TNRO','KAYNTI_ALKOI','KAYNTI_LOPPUI','KAYNTI_YHTEYSTAPA','KAYNTI_PALVELUMUOTO','KAYNTI_AMMATTI']
AvoHilmo_pt2_col2keep		= ['FINREGISTRYID','SOURCE','ICDVER','CATEGORY','INDEX','EVENT_AGE','CODE1','CODE2','CODE3','CODE4','CODE5','CODE6','CODE7']

#--------------------


def KelaPurchasePreprocessing(file_path:str,file_sep:str):

	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# select desired columns 
	SubsetData = OriginalData[ KelaPurcahse_col2keep ]
	# rename columns
	SubsetData.rename( 
		columns = {
		'ATC_CODE':'CODE1',
		'SAIR':'CODE2',
		'VNRO':'CODE3',
		'PLKM':'CODE4',
		'KORV':'CODE5',
		'KAKORV':'CODE6',
		'LAJI':'CODE7',
		'LAAKEOSTPVM':'PVM'
		},
		inplace=True)

	# missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	NewData = SubsetData.loc[ !( SubsetData.CODE1.isna() & SubsetData.CODE2.isna() )] 

	# remove duplicates ?
	...

	# add columns
	NewData['INDEX'] 			= np.arange( 1, NewData.shape[0]+1 )
	NewData['SOURCE'] 			= 'PURCH'
	NewData['CATEGORY'] 		= np.NaN
	NewData['ICDVER']			= np.NaN
	NewData['EVENT_YRMNTH']		= NewData['PVM'][:7]

	# QC part

	# fix CODE3 (VNR code) manually
	...

	return NewData


def KelaReimbursementPreprocessing(file_path:str,file_sep:str):

	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# select desired columns 
	SubsetData = OriginalData[ KelaReimbursement_col2keep ]
	# rename columns
	SubsetData.rename( 
		columns = {
		'KELA_DISEASE':'CODE1',
		'ICD':'CODE2',
		'LAAKEOSTPVM':'PVM'
		},
		inplace=True)

	# missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	NewData = SubsetData.loc[ !( SubsetData.CODE1.isna() & SubsetData.CODE2.isna() )]

	# remove duplicates ?
	...

	# add columns
	NewData['INDEX'] 			= np.arange( 1, NewData.shape[0]+1 )
	NewData['SOURCE'] 			= 'REIMB'
	NewData['CATEGORY'] 		= np.NaN
	NewData['CODE3']			= np.NaN
	NewData['CODE4']			= np.NaN
	NewData['EVENT_YRMNTH']		= NewData['PVM'][:7]


	# QC part
	# remove ICD code dots
	NewData['CODE2'] = NewData['CODE2'].replace({".", ""})

	return NewData


def CancerRegistryPreprocessing(file_path:str,file_sep:str):

	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# select desired columns 
	SubsetData = OriginalData[ CancerRegistry_col2keep]

	# add columns
	SubsetData['INDEX'] 		= np.arange( 1, SubsetData.shape[0]+1 )
	SubsetData['SOURCE'] 		= 'CANC'
	SubsetData['ICD_VER'] 		= 'O3'
	SubsetData['CATEGORY'] 		= np.NaN
	SubsetData['CODE4']			= np.NaN
	SubsetData['EVENT_YRMNTH']	= SubsetData['dg_date'].to_string()[:7]

	# rename columns
	SubsetData.rename( 
		columns = {
		'topo':'CODE1',
		'morpho':'CODE2',
		'beh':'CODE3',
		'dg_date':'PVM'
		},
		inplace=True)

	# missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	NewData = SubsetData.loc[ !( SubsetData.CODE1.isna() & SubsetData.CODE2.isna() )] 

	# remove duplicates ?
	...

	return NewData

def CauseOfDeathPreprocessing(file_path:str,file_sep:str):

	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# select desired columns 
	SubsetData = OriginalData[ CauseOfDeath_col2keep ]

	# add columns
	SubsetData['INDEX'] 		= np.arange( 1, SubsetData.shape[0]+1 )
	SubsetData['SOURCE'] 		= 'DEATH'
	SubsetData['CODE2']			= np.NaN
	SubsetData['CODE3']			= np.NaN
	SubsetData['CODE4']			= np.NaN
	SubsetData['EVENT_YRMNTH']	= SubsetData['dg_date'].to_string()[:7]

	# add category depending on what ? 
	SubsetData['CATEGORY'] 		=  ...

	# rename columns
	SubsetData.rename( 
		columns = {
		'topo':'CODE1',
		'morpho':'CODE2',
		'beh':'CODE3',
		'KUOLPVM':'PVM'
		},
		inplace=True)

	# missing values
	SubsetData.loc[SubsetData==''] = np.NaN
	NewData = SubsetData.loc[ !( SubsetData.CODE1.isna() & SubsetData.CODE2.isna() )] 

	# remove duplicates ?
	...

	return NewData




def HilmoInpat_PRE95_Preprocessing(file_path:str,file_sep:str):

	# NB: data used was created here: script_1.py and is using ICD 8 + 9 codes
	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# select desired columns 
	SubsetData = OriginalData[ Hilmo_69_95_col2keep ]

	# add columns
	SubsetData['INDEX'] 		= np.arange( 1, SubsetData.shape[0]+1 )
	SubsetData['SOURCE'] 		= 'INPAT'
	SubsetData['CODE3']			= np.NaN
	SubsetData['CODE4']			= SubsetData.LAHTOPVM - SubsetData.TULOPVM
	SubsetData['EVENT_YRMNTH']	= SubsetData['TULOPVM'].to_string()[:7]

	#rename columns
	SubsetData.rename( columns = {'TULOPVM':'PVM'}, inplace=True )

	# add category depending on what ? 
	SubsetData['CATEGORY'] 		=  ...
	
	# QC part
	# check negative hospital days ? -> put NA 
	NewData = ...

	return NewData

def HilmoInpat_POST95_Preprocessing(file_path:str,file_sep:str):

	# NB: this part of Hilmo is using ICD10 codes
	OriginalData = pd.read_csv(file_path,sep = file_sep, encoding='latin-1')

	# select desired columns 
	SubsetData = OriginalData[ Hilmo_POST95_col2keep ]

	#rename columns
	SubsetData.rename( columns = {'TULOPVM':'PVM'}, inplace=True )
	
	# QC part
	# remove wrong codes
	wrong_codes = ['H','M','N','Z6','ZH','ZZ']
	NewData = SubsetData.loc[SubsetData.PALA in wrong_codes]

	return NewData


	

def HilmoOperationsPreprocessing(file_path:str,file_sep:str):

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




def HilmoPreprocessing_pt2():
	
	return ... 




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

	return ...
