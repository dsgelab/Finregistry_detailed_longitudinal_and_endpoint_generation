
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry 2023  
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################


from datetime import datetime
import gc

# import all processing functions
from func import *

# import all file paths
from config import *

# import info on Date_Of_Birth and Date_Of_Death 
BIRTH_DEATH_MAP = pd.read_csv('/data/processed_data/minimal_phenotype/minimal_phenotype_2023-05-02.csv',sep = ',', encoding='latin-1')
BIRTH_DEATH_MAP = BIRTH_DEATH_MAP[['FINREGISTRYID','date_of_birth','death_date']]


##########################################################
# CREATE DETAILED LONGITUDINAL 

if __name__ == '__main__':

	#--------------------------------------
	# HILMO

	print('start processing hilmo files')
	START = datetime.now()

	Hilmo_69_86_processing(hilmo_1969_1986, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_87_93_processing(hilmo_1987_1993, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_94_95_processing(hilmo_1994_1995, DOB_map=BIRTH_DEATH_MAP)

	print('hilmo pre 95 completed')
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	START = datetime.now()

	oper_96_18 = Hilmo_operations_preparation(hilmo_oper_1996_2018, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=oper_96_18)
	del oper_96_18
	gc.collect() 

	oper_19_21 = Hilmo_operations_preparation(hilmo_oper_2019_2021, DOB_map=BIRTH_DEATH_MAP)
	Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=oper_19_21) 
	del oper_19_21
	gc.collect() 

	print('hilmo post 95 + operation completed')
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	START = datetime.now()

	#heart_94_95 = Hilmo_heart_preparation(hilmo_heart_1994_1995)
	heart_96_18 = Hilmo_heart_preparation(hilmo_heart_1996_2018)
	Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=heart_96_18)
	del heart_96_18
	gc.collect() 

	heart_19_21 = Hilmo_heart_preparation(hilmo_heart_2019_2021)
	Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=heart_19_21)
	del heart_19_21
	gc.collect() 

	print('hilmo post 95 + heart completed')
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	START = datetime.now()

	diag_96_18 = Hilmo_diagnosis_preparation(hilmo_diag_1996_2018)
	Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=diag_96_18)
	del diag_96_18
	gc.collect() 

	diag_19_21 = Hilmo_diagnosis_preparation(hilmo_diag_2019_2021)
	Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=diag_19_21) 
	del diag_19_21
	gc.collect() 

	print('hilmo post 95 + diagnosis completed')
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')

	#--------------------------------------
	# AVOHILMO

	avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016,avohilmo_2017_2018,avohilmo_2019_2020,avohilmo_2020,avohilmo_2021]
	print('start processing avohilmo files')
	START = datetime.now()

	icd10_11_16 = AvoHilmo_icd10_preparation(avohilmo_icd10_2011_2016)
	icd10_17_19 = AvoHilmo_icd10_preparation(avohilmo_icd10_2017_2019)
	icd10_20_21 = AvoHilmo_icd10_preparation(avohilmo_icd10_2020_2021)
	#concat everything togheter 
	icd10 = pd.concat([icd10_11_16,icd10_17_19,icd10_20_21])

	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=icd10)

	print('avohilmo + icd10 completed')
	del icd10
	gc.collect() 
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	START = datetime.now()		

	icpc2_11_16 = AvoHilmo_icpc2_preparation(avohilmo_icpc2_2011_2016)
	icpc2_17_19 = AvoHilmo_icpc2_preparation(avohilmo_icpc2_2017_2019)
	icpc2_20_21 = AvoHilmo_icpc2_preparation(avohilmo_icpc2_2020_2021)
	#concat everything togheter
	icpc2 = pd.concat([icpc2_11_16,icpc2_17_19,icpc2_20_21])

	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=icpc2)

	print('avohilmo + icpc2 completed')
	del icpc2
	gc.collect() 
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	START = datetime.now()	

	oral_11_16 = AvoHilmo_dental_measures_preparation(avohilmo_oral_2011_2016)
	oral_17_19 = AvoHilmo_dental_measures_preparation(avohilmo_oral_2017_2019)
	oral_20_21 = AvoHilmo_dental_measures_preparation(avohilmo_oral_2020_2021)
	#concat everything togheter
	oral = pd.concat([oral_11_16,oral_17_19,oral_20_21])

	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=BIRTH_DEATH_MAP, extra_to_merge=oral)

	print('avohilmo + oral completed')
	del oral
	gc.collect()  
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	START = datetime.now()		

	oper_11_16 = AvoHilmo_interventions_preparation(avohilmo_oper_2011_2016)
	oper_17_19 = AvoHilmo_interventions_preparation(avohilmo_oper_2017_2019)
	oper_20_21 = AvoHilmo_interventions_preparation(avohilmo_oper_2020_2021)
	#concat everything togheter
	oper = pd.concat([oper_11_16,oper_17_19,oper_20_21])

	print('avohilmo + oper completed')
	del oper
	gc.collect() 
	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	
	#--------------------------------------
	# OTHER REGISTRIES

	print('start processing death registry')
	START = datetime.now()

	DeathRegistry_processing(death,DOB_map=BIRTH_DEATH_MAP)

	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	print('start processing cancer registry')
	START = datetime.now()

	CancerRegistry_processing(cancer,DOB_map=BIRTH_DEATH_MAP)

	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	print('start processing kela reimbursement')
	START = datetime.now()

	KelaReimbursement_PRE20_processing(kela_reimbursement_pre2020, DOB_map=BIRTH_DEATH_MAP)
	KelaReimbursement_20_21_processing(kela_reimbursement_2020_2021, DOB_map=BIRTH_DEATH_MAP)

	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')
	print('start processing kela purchase')
	START = datetime.now()

	for purchase_file in kela_purchase_filelist:
		KelaPurchase_processing(purchase_file,DOB_map=BIRTH_DEATH_MAP)

	END = datetime.now()
	print(f'the processing took {(END-START)} hour:min:sec')


