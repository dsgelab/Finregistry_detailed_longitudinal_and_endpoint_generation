
# DEFINE PATHS (in Finregistry):

THL_HILMO_PATH = '/data/original_data/thl_hilmo/'

hilmo_1969_1986 			= THL_HILMO_PATH+'thl2019_1776_poisto_6986.csv.finreg_IDs'
hilmo_1987_1993 			= THL_HILMO_PATH+'thl2019_1776_poisto_8793.csv.finreg_IDs'
hilmo_1994_1995  			= THL_HILMO_PATH+'thl2019_1776_hilmo_9495.csv.finreg_IDs' 
hilmo_1995_2018  			= THL_HILMO_PATH+'thl2019_1776_hilmo.csv.finreg_IDs' 
hilmo_2019_2021  			= THL_HILMO_PATH+'THL2021_2196_HILMO_2019_2021.csv.finreg_IDs' 

hilmo_diag_1995_2018 		= THL_HILMO_PATH+'thl2019_1776_hilmo_diagnoosit_kaikki.csv.finreg_IDs'
hilmo_diag_2019_2021		= THL_HILMO_PATH+'THL2021_2196_HILMO_DIAG.csv.finreg_IDs'

hilmo_oper_1995_2018		= THL_HILMO_PATH+'thl2019_1776_hilmo_toimenpide.csv.finreg_IDs'
hilmo_oper_2019_2021		= THL_HILMO_PATH+'THL2021_2196_HILMO_TOIMP.csv.finreg_IDs'

hilmo_heart_1994_1995		= THL_HILMO_PATH+'thl2019_1776_hilmo_9495_syp.csv.finreg_IDs '
hilmo_heart_1995_2018		= THL_HILMO_PATH+'thl2019_1776_hilmo_syp.csv.finreg_IDs'
hilmo_heart_2019_2021		= THL_HILMO_PATH+'THL2021_2196_HILMO_SYP.csv.finreg_IDs'

THL_AVOHILMO_PATH = '/data/original_data/thl_avohilmo/'

avohilmo_icd10_2011_2016	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_icd10.csv.finreg_IDs'
avohilmo_icd10_2017_2019	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_icd10.csv.finreg_IDs'
avohilmo_icd10_2020_2021	= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_ICD10_DIAG.csv.finreg_IDs'

avohilmo_icpc2_2011_2016	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_icpc2.csv.finreg_IDs'
avohilmo_icpc2_2017_2019	= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_icpc2.csv.finreg_IDs '
avohilmo_icpc2_2020_2021	= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_ICPC2_DIAG.csv.finreg_IDs'

avohilmo_oral_2011_2016		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_suu_toimenpide.csv.finreg_IDs'
avohilmo_oral_2017_2019		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_suu_toimp.csv.finreg_IDs'
avohilmo_oral_2020_2021		= THL_AVOHILMO_PATH+'thl2021_2196_avohilmo_suu_toimp.csv.finreg_IDs'

avohilmo_oper_2011_2016		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_toimenpide.csv.finreg_IDs'
avohilmo_oper_2017_2019		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_20_toimenpide.csv.finreg_IDs'
avohilmo_oper_2020_2021		= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_TOIMP.csv.finreg_IDs'

avohilmo_2011_2012 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_11_12.csv.finreg_IDs'
avohilmo_2013_2014 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_13_14.csv.finreg_IDs'
avohilmo_2015_2016  		= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_15_16.csv.finreg_IDs'
avohilmo_2017_2018 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_17_18.csv.finreg_IDs'
avohilmo_2019_2020 			= THL_AVOHILMO_PATH+'thl2019_1776_avohilmo_19_20.csv.finreg_IDs'
avohilmo_2020  				= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_2020.csv.finreg_IDs'
avohilmo_2021  				= THL_AVOHILMO_PATH+'THL2021_2196_AVOHILMO_2021.csv.finreg_IDs'

death 						= '/data/original_data/sf_death/thl2019_1776_ksyy_tutkimus.csv.finreg_IDs'

cancer 						= '/data/original_data/thl_cancer/fcr_data.csv.finreg_IDs'

kela_reimburement 			= '/data/original_data/kela_reimbursement/175_522_2020_LAAKEKORVAUSOIKEUDET.csv.finreg_IDs'
kela_purchase_filelist		= ['/data/original_data/kela_purchase/175_522_2020_LAAKEOSTOT_'+str(n)+'.csv.finreg_IDs' for n in range(1995,2020)]


DETAILED_LONGITUDINAL_PATH 	= '/data/processed_data/detailed_longitudinal/R10/service_sector/'
TEST_FOLDER_PATH 			= '/home/mferro/service_sector_update/'