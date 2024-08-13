
# Detailed longitudinal

This repository contains the code for creating the detailed longitudinal file in Finregistry. The code have been transposed starting from the original FinnGen one ([GitHub repository](https://github.com/FINNGEN/service-sector-data-processing/tree/master)) with some minor changes, described in section *FinRegistry vs. FinnGen*. 

## Repository structure


- `main.py` performs all the processing required in order to create the detailed longitudial splits
- `test.py` performs all the test required to check that all the function used in the `main.py` file are working properly
- `join_splits.sh` is the bash script responsible for merging together all the splits and create the official version of detailed longitudinal file
- the `utils/` folder contains all extra scripts necessary, includes: `config.py` and `config_test.py` containing all data file paths to be used, as well as `func.py` where all the functions to be used are defined

## Test Pipeline

Dummy data for testing the script is available in the `test_data/` folder.
This data was generated using the `LOIRE` package from Ida Holopainen and modified to fit our format requirements.

Default parameter have been used, but the following changes:
n=1000, seed=1722463180, add_offset=False


## Output format

The output includes the following columns. 

- `FINREGISTRYID`
- `SOURCE`
- `EVENT_AGE`
- `EVENT_DAY`
- `CODE1`
- `CODE2`
- `CODE3`
- `CODE4`
- `CODE5`
- `CODE6`
- `CODE7`
- `CODE8`
- `CODE9`
- `ICDVER`
- `CATEGORY`
- `INDEX`

The description of each column depends on the source registry. Please refer to the data catalog for more details. 

# Registries included

Detailed longitudinal covers data from the following registries. 

| Registry        | Start date | End date |
|-----------------|------------|----------|
| Hilmo           | Jan 1969   | Dec 2021 |
| AvoHilmo		  | Jan 2011   | Dec 2021 |
| Cancer          | Jan 1953   | Dec 2020 |
| Death           | Jan 1971   | Dec 2020 |
| Purchases       | Jan 1995   | Dec 2021 |
| Reimbursements  | Jan 1964   | Dec 2021 |

Please note that the first data point might be before or after the official start date of each registry. 

## Processing details

### General rules

For all the registries, we exclude rows based on the following rules: 
- `EVENT_DATE` must be before `DEATH_DATE`
- `EVENT_AGE` is not missing 
- `EVENT_AGE` must be between 0 and 110
- `CODE1` or `CODE2` is available 
- `CODE4` must be a non-negative number 
- If the row includes an ATC/VNRO code, the code must be correctly formated

Please also note that the `EVENT_AGE` is round up to two decimal points. 

### FinRegistry vs. FinnGen

The following changes have been made to the FinnGen version of the detailed longitudinal preprocessing pipeline to enable running it in FinRegistry. Otherwise the processing pipeline should be identical to the one in FinnGen. Please refer to the service sector data processing [GitHub repo](https://github.com/FINNGEN/service-sector-data-processing/tree/master) for more information. 

- The code is restructured to enable multiprocessing
- No separate steps for adding FinRegistry ID as they are already available in the FinRegistry data
- No individuals are removed from the dataset (FinnGen removes individuals in the ID denials list)
- No separate step for computing the birth and death dates as they are already included in the FinRegistry data
- Index now reference the exact line in the original registry file that was used for extracting the information
- No dot is removed from CODE columns
- No CODE4 (Duration of hospital stay) value is removed


Please also note that age randomization is not implemented in FinRegistry so all the dates are exact.


### Hilmo 

Throughout the registry existence three ICD disease classification versions changed (8 to 10):
- ICD-8: From the start of the register up to the end of 1986
- ICD-9: From 1987-01-01 to the end of 1995
- ICD-10: From 1996-01-01 up to now

In addition to that, for the period 1994-1995 there is a separate Hilmo file with ICD-9 codes, this is due to the change of coding within the register in 1994 (not due to the change in ICD version).

Finnish Hospital "league codes" were used until 1996 when the use of Nomesco codes started.

Since 1998 the register also contains outpatient care codes (inpatient and outpatient codes can be distinguished from the SOURCE column of detailed longitudinal). The inpatient/outpatient split is made according to `PALA` from 1998 to 2019 and `YHTEYSTAPA` + `PALA` codes from 2019 to 2021.   

NB: see functions `Define_INPAT()`, `Define_OPERIN()` and `Define_OPEROUT()` in `func.py`

In addition to the hilmo inpatient and outpatient files we have information about:
- diagnostic ICD codes (Hilmo diagnosis) to be joined to hilmo after 1995, before that year the codes where already present in the main hilmo dataset.
- heart surgery codes (Hilmo heart, recorded from 1994)
- other surgical codes (Hilmo operations) referring to day hospital operations. 

For a period up to 2019, a small correction is made inside Hilmo heart to the `CATEGORY` variable which records a source of a code.

- HPO1:3 - Procedure for demanding heart patient, old coding
- HPN1:N - Procedure for demanding heart patient, new coding 

Some HPO (old) fully numeric codes were mixed in with HPN (new) codes always starting with the letter "A". HPN is changed to HPO for fully numeric codes.

Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained special characters (`+`, `*`, `#`, `@`) which were also processed. For more info check the function `CombinationCodesSplit()` in `func.py`. 

NB: for quality control reasons, the `CATEGORY` value for ICD codes will be limited between 0 and 3, where 0 is going to represent the main diagnosis and 1, 2, 3 are side diagnosis.

### AvoHilmo

Register codes given in the primary health care visits are not as confirmed as codes given in hospital (inpatient data) or codes coming from the specialized outpatient visits (outpatient data). Finnish doctors are legally responsible for ICD codes in Hilmo, but Avohilmo codes do not carry the same responsibility. Avohilmo data includes also codes that are given by nurse (ICPC2), these codes include procedures as well.

Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained a special characters {+,\*,#,@} which were also removed. 

In addition to icd10 and icpc2 information, there are files (to be joined with avohilmo) referring to dental measures and interventions.

### Cancer Registry

See the code for details. 

### Death Registry

Between 1969 to 1986, the international classification ICD-8 was in use, with Finnish additions. Some Finnish additions to the ICD-codes can be found [here](https://taika.stat.fi/en/aineistokuvaus.html#!?dataid=ksyyt_197100_jua_kuolemansyyt_001.xml).

Between 1987 and 1995, the data were classified using the national classification of diseases ICD9, where comparability to international version is maintained.

Since 1996, the statistics have been compiled based on the 10th revision of the International Classification of Diseases (ICD-10), with some Finnish additions (listed [here](https://taika.stat.fi/en/aineistokuvaus.html#!?dataid=ksyyt_197100_jua_kuolemansyyt_001.xml) ). 
Note that this is the WHO version of ICD10 and therefore does not include some of the subtypes/extensions of the ICD10 codes used in the full Finnish se of ICD codes.

There are six columns contain ICD codes: 
The basic cause of death (TPKS), the immediate cause of death (VKS) and four contributing causes of death (M1-M4). All those codes are transferred to the ‘CODE1’ column of detailed longitudinal with an appropriate 'CATEGORY' label denoting a code type. 

### Kela Purchases

Some light data cleaning was done: 
- Removal of entries which did not contain either ATC code or Kela reimbursement code (SAIR)
- Removed duplicates 
- All VNRO codes to be without "." and 6-digit long 
- Death date from COD was used to correct event dates recorded after death date (those dates were changed to death date)

### Kela Reimbursement

See the code for details. 

## Extra information

### FinnGen handbook

More info on the registers used for detailed longitudinal: 
https://finngen.gitbook.io/finngen-analyst-handbook/finngen-data-specifics/red-library-data-individual-level-data/what-phenotype-files-are-available-in-sandbox-1/detailed-longitudinal-data/registers-in-the-detailed-longitudinal-data

More info on the CODEs names/meaning:
https://finngen.gitbook.io/finngen-analyst-handbook/finngen-data-specifics/finnish-health-registers-and-medical-coding/international-and-finnish-health-code-sets

### A note on causes of death pre-processing

We have received two files "thl2021_2196_ksyy_tutkimus" and "thl2021_2196_ksyy_vuosi". 
The first larger file (tutkimus) contains information for all IDs from a second file (vuosi) and also contains death dates (but no medical codes) for additional 15467 IDs.  
Trying to match IDs between files there are some small differences in variables (e.g. for 15 IDs different death date is recorded, and for 0.1% IDs cause of death codes differ). As we don’t know which information is correct and differences are only for a small portion of IDs, using only tutkimus seems appropriate.

### A note on the updated Kela reimbursements file

A data update file despite containing reimbursement information for two additional years (2020-2021) had considerably fewer entries compared to an old file. This in part was due to the removal of rows with missing information (rows with missing full dates were removed in data update file).

Also column names have been changed in the meanwhile

## Contact

For any information about the detailed longitudinal or the data processing pipeline, please contact Matteo Ferro (matteo.ferro@helsinki.fi) or Essi Viippola (essi.viippola@helsinki.fi). 

If you discover any bugs or have any comments or suggestions, please open an issue on this GitHub repo. 