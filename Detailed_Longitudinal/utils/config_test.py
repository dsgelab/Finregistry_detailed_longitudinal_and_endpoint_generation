
from pathlib import Path

# DEFINE PATHS :

INPUT_DATA_PATH = Path('/home/mferro/service_sector_update/new_test_pipeline/test_data/inputs')
OUTPUT_DATA_PATH = Path('/home/mferro/service_sector_update/new_test_pipeline/test_data/outputs')
RESULT_DATA_PATH = Path('/home/mferro/service_sector_update/new_test_pipeline/test_results')

DOB_map_file = INPUT_DATA_PATH/'base_dt_toy_data.csv'
PALTU_map_file = '/home/mferro/service_sector_update/PALTU_mapping.csv'

cancer_input = INPUT_DATA_PATH/'cancer_toy_data.csv'
cancer_output = OUTPUT_DATA_PATH/'cancer_long_qc.csv'
cancer_result = RESULT_DATA_PATH/'Cancer_test.csv'

death_input = INPUT_DATA_PATH/'death_toy_data.csv'
death_output = OUTPUT_DATA_PATH/'deaths_long_qc.csv'
death_result = RESULT_DATA_PATH/'Death_test.csv'

purch_input = INPUT_DATA_PATH/'purch_med_1993_toy_data.csv'
purch_output = OUTPUT_DATA_PATH/'purch_med_long_qc.csv'
purch_result = RESULT_DATA_PATH/'KelaPurchase_test.csv'

reimb_input = INPUT_DATA_PATH/'reimb_med_toy_data.csv'
reimb_output = OUTPUT_DATA_PATH/'reimb_med_long_qc.csv'
reimb_result = RESULT_DATA_PATH/'KelaReimbursement_test.csv'

hilmo_69_86_input = INPUT_DATA_PATH/'hilmo_69_86_toy_data.csv'
hilmo_69_86_output = OUTPUT_DATA_PATH/'hilmo_69_86_long_qc.csv'
hilmo_69_86_result = RESULT_DATA_PATH/'Hilmo_1969_1986_test.csv'

hilmo_87_93_input = INPUT_DATA_PATH/'hilmo_87_93_toy_data.csv'
hilmo_87_93_output = OUTPUT_DATA_PATH/'hilmo_87_93_long_qc.csv'
hilmo_87_93_result = RESULT_DATA_PATH/'Hilmo_1987_1993_test.csv'

hilmo_94_95_input = INPUT_DATA_PATH/'hilmo_94_95_toy_data.csv'
hilmo_94_95_output = OUTPUT_DATA_PATH/'hilmo_94_95_long_qc.csv'
hilmo_94_95_result = RESULT_DATA_PATH/'Hilmo_1994_1995_test.csv'

hilmo_96_18_base_input = INPUT_DATA_PATH/'hilmo_96_18_toy_data.csv'
hilmo_post18_base_input = INPUT_DATA_PATH/'hilmo_post18_toy_data.csv'

hilmo_diag_input = INPUT_DATA_PATH/'hilmo_extra_diag_toy_data.csv'
hilmo_oper_input = INPUT_DATA_PATH/'hilmo_oper_toy_data.csv'
hilmo_heart_input = INPUT_DATA_PATH/'hilmo_heart_toy_data.csv'

hilmo_diag_output = OUTPUT_DATA_PATH/'hilmo_96_18_diag_long_qc.csv'
hilmo_oper_output = OUTPUT_DATA_PATH/'hilmo_96_18_oper_long_qc.csv'
hilmo_heart_output = OUTPUT_DATA_PATH/'hilmo_96_18_heart_long_qc.csv'

hilmo_diag_result = RESULT_DATA_PATH/'Hilmo_1996_2018_diag_test.csv'
hilmo_oper_result = RESULT_DATA_PATH/'Hilmo_1996_2018_oper_test.csv'
hilmo_heart_result = RESULT_DATA_PATH/'Hilmo_1996_2018_heart_test.csv'

hilmo_post18_diag_output = OUTPUT_DATA_PATH/'hilmo_post_18_diag_long_qc.csv'
hilmo_post18_oper_output = OUTPUT_DATA_PATH/'hilmo_post_18_oper_long_qc.csv'
hilmo_post18_heart_output = OUTPUT_DATA_PATH/'hilmo_post_18_heart_long_qc.csv'

hilmo_post18_diag_result = RESULT_DATA_PATH/'Hilmo_2019_2021_diag_test.csv'
hilmo_post18_oper_result = RESULT_DATA_PATH/'Hilmo_2019_2021_oper_test.csv'
hilmo_post18_heart_result = RESULT_DATA_PATH/'Hilmo_2019_2021_heart_test.csv'

avohilmo_base_input = INPUT_DATA_PATH/'prim_care_toy_data.csv'

avohilmo_icd10_input = INPUT_DATA_PATH/'prim_care_icd10_toy_data.csv'
avohilmo_icd10_output = OUTPUT_DATA_PATH/'prim_care_icd10_long_qc.csv'
avohilmo_icd10_result = RESULT_DATA_PATH/'Avohilmo_icd10__test.csv'

avohilmo_icpc2_input = INPUT_DATA_PATH/'prim_care_icpc2_toy_data.csv'
avohilmo_icpc2_output = OUTPUT_DATA_PATH/'prim_care_icpc2_long_qc.csv'
avohilmo_icpc2_result = RESULT_DATA_PATH/'Avohilmo_icpc2__test.csv'

avohilmo_oral_input = INPUT_DATA_PATH/'prim_care_dent_toy_data.csv'
avohilmo_oral_output = OUTPUT_DATA_PATH/'prim_care_dent_long_qc.csv'
avohilmo_oral_result = RESULT_DATA_PATH/'Avohilmo_oral__test.csv'

avohilmo_oper_input = INPUT_DATA_PATH/'prim_care_oper_toy_data.csv'
avohilmo_oper_output = OUTPUT_DATA_PATH/'prim_care_oper_long_qc.csv'
avohilmo_oper_result = RESULT_DATA_PATH/'Avohilmo_oper__test.csv'

