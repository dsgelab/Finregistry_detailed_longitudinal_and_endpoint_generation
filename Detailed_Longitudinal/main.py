
##########################################################
# COOPYRIGHT:  	THL/FIMM/Finregistry 2023  
# AUTHORS:     	Matteo Ferro, Essi Vippola
##########################################################


from datetime import datetime
import pandas as pd
import gc

# import all processing functions
from func import *

# import all file paths
from config import *

# fetch info to add later: 
DOB_map = DOB_map_preparation('/data/processed_data/minimal_phenotype/minimal_phenotype_2023-05-02.csv', sep=',')
paltu_map = pd.read_csv("PALTU_mapping.csv", sep=",")


##########################################################
# CREATE DETAILED LONGITUDINAL 

if __name__ == '__main__':

	#--------------------------------------
	# HILMO

# ---

	START = datetime.now()
	Hilmo_69_86_processing(hilmo_1969_1986, DOB_map=DOB_map)
	END = datetime.now()
	print(f'hilmo_1969_1986 processing took {(END-START)} hour:min:sec')


	START = datetime.now()
	Hilmo_87_93_processing(hilmo_1987_1993, DOB_map=DOB_map, paltu_map=paltu_map)
	END = datetime.now()
	print(f'hilmo_1987_1993 processing took {(END-START)} hour:min:sec')

# ---

	START = datetime.now()
	heart_94_95 = Hilmo_heart_preparation(hilmo_heart_1994_1995)
	Hilmo_94_95_processing(hilmo_1994_1995, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_94_95)
	END = datetime.now()
	print(f'hilmo_1994_1995 + heart processing took {(END-START)} hour:min:sec')
	del heart_94_95
	gc.collect() 


	START = datetime.now()
	heart_96_18 = Hilmo_heart_preparation(hilmo_heart_1996_2018)
	Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_96_18)
	END = datetime.now()
	print(f'hilmo_1996_2018 + heart processing took {(END-START)} hour:min:sec')	
	del heart_96_18
	gc.collect() 


	START = datetime.now()
	heart_19_21 = Hilmo_heart_preparation(hilmo_heart_2019_2021)
	Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=heart_19_21)
	END = datetime.now()
	print(f'hilmo_2019_2021 + heart processing took {(END-START)} hour:min:sec')
	del heart_19_21
	gc.collect() 

# ---

	START = datetime.now()
	oper_96_18 = Hilmo_operations_preparation(hilmo_oper_1996_2018)
	Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_96_18)
	END = datetime.now()
	print(f'hilmo_1996_2018 + oper processing took {(END-START)} hour:min:sec')
	del oper_96_18
	gc.collect() 


	START = datetime.now()
	oper_19_21 = Hilmo_operations_preparation(hilmo_oper_2019_2021)
	Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=oper_19_21) 
	END = datetime.now()
	print(f'hilmo_2019_2021 + oper processing took {(END-START)} hour:min:sec')
	del oper_19_21
	gc.collect() 

# ---

	START = datetime.now()
	diag_96_18 = Hilmo_diagnosis_preparation(hilmo_diag_1996_2018)
	Hilmo_96_18_processing(hilmo_1996_2018, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=diag_96_18)
	END = datetime.now()
	print(f'hilmo_1996_2018 + diag processing took {(END-START)} hour:min:sec')
	del diag_96_18
	gc.collect() 


	START = datetime.now()
	diag_19_21 = Hilmo_diagnosis_preparation(hilmo_diag_2019_2021)
	Hilmo_POST18_processing(hilmo_2019_2021, DOB_map=DOB_map, paltu_map=paltu_map, extra_to_merge=diag_19_21) 
	END = datetime.now()
	print(f'hilmo_2019_2021 + diag processing took {(END-START)} hour:min:sec')
	del diag_19_21
	gc.collect() 


	#--------------------------------------
	# AVOHILMO
	avohilmo_to_process = [avohilmo_2011_2012,avohilmo_2013_2014,avohilmo_2015_2016,avohilmo_2017_2018,avohilmo_2019_2020,avohilmo_2020,avohilmo_2021]

# ---

	START = datetime.now()

	icd10_11_16 = AvoHilmo_codes_preparation(avohilmo_icd10_2011_2016, source='icd10')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_11_16)
	del icd10_11_16
	gc.collect() 
	icd10_17_19 = AvoHilmo_codes_preparation(avohilmo_icd10_2017_2019, source='icd10')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_17_19)
	del icd10_17_19
	gc.collect() 
	icd10_20_21 = AvoHilmo_codes_preparation(avohilmo_icd10_2020_2021, source='icd10')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icd10_20_21)
	del icd10_20_21
	gc.collect() 

	END = datetime.now()
	print(f'avohilmo + icd10 processing took {(END-START)} hour:min:sec')

# ---

	START = datetime.now()		

	icpc2_11_16 = AvoHilmo_codes_preparation(avohilmo_icpc2_2011_2016, source='icpc2')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_11_16)
	del icpc2_11_16
	gc.collect() 
	icpc2_17_19 = AvoHilmo_codes_preparation(avohilmo_icpc2_2017_2019, source='icpc2')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_17_19)	
	del icpc2_17_19
	gc.collect() 
	icpc2_20_21 = AvoHilmo_codes_preparation(avohilmo_icpc2_2020_2021, source='icpc2')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=icpc2_20_21)	
	del icpc2_20_21
	gc.collect() 

	END = datetime.now()
	print(f'avohilmo + icpc2 processing took {(END-START)} hour:min:sec')

# ---

	START = datetime.now()	

	oral_11_16 = AvoHilmo_codes_preparation(avohilmo_oral_2011_2016, source='oral')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_11_16)
	del oral_11_16
	gc.collect() 
	oral_17_19 = AvoHilmo_codes_preparation(avohilmo_oral_2017_2019, source='oral')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_17_19)
	del oral_17_19
	gc.collect() 
	oral_20_21 = AvoHilmo_codes_preparation(avohilmo_oral_2020_2021, source='oral')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oral_20_21)
	del oral_20_21
	gc.collect() 

	END = datetime.now()
	print(f'avohilmo + oral processing took {(END-START)} hour:min:sec')

# ---

	START = datetime.now()	

	oper_11_16 = AvoHilmo_codes_preparation(avohilmo_oper_2011_2016, source='oper')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_11_16)	
	del oper_11_16
	gc.collect() 
	oper_17_19 = AvoHilmo_codes_preparation(avohilmo_oper_2017_2019, source='oper')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_17_19)	
	del oper_17_19
	gc.collect() 
	oper_20_21 = AvoHilmo_codes_preparation(avohilmo_oper_2020_2021, source='oper')
	for avohilmo in avohilmo_to_process:
		AvoHilmo_processing(avohilmo, DOB_map=DOB_map, extra_to_merge=oper_20_21)
	del oper_20_21
	gc.collect() 	

	END = datetime.now()
	print(f'avohilmo + oper processing took {(END-START)} hour:min:sec')
	
	#--------------------------------------
	# OTHER REGISTRIES


	START = datetime.now()
	DeathRegistry_processing(death_pre2020,DOB_map=DOB_map)
	DeathRegistry_processing(death_2020_2021,DOB_map=DOB_map)
	END = datetime.now()
	print(f'death registry processing took {(END-START)} hour:min:sec')

	START = datetime.now()
	CancerRegistry_processing(cancer,DOB_map=DOB_map)
	END = datetime.now()
	print(f'cancer registry processing took {(END-START)} hour:min:sec')

	START = datetime.now()
	KelaReimbursement_PRE20_processing(kela_reimbursement_pre2020, DOB_map=DOB_map)
	KelaReimbursement_20_21_processing(kela_reimbursement_2020_2021, DOB_map=DOB_map)
	END = datetime.now()
	print(f'kela reimbursement processing took {(END-START)} hour:min:sec')

	START = datetime.now()
	for purchase_file in kela_purchase_pre2020:
		KelaPurchase_PRE20_processing(purchase_file,DOB_map=DOB_map)
	for purchase_file in kela_purchase_2020_2021:
		KelaPurchase_20_21_processing(purchase_file,DOB_map=DOB_map)
	END = datetime.now()
	print(f'kela purchases processing took {(END-START)} hour:min:sec')

# ---

	# DROP DUPLICATES: to be performed with awk script

# ---

