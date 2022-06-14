This repository contains scripts for initial registry pre-processing (only registers which are used for endpoint generation), scripts to transform data into “detailed longitudinal” format and supporting code for running endpointer.py and transforming endpoint files into densified versions.

# Pre-processing
Small pre-processing steps (e.g. date format harmonisation, duplicate removal is performed here). Neither number of columns, nor column names are changed from original.
All pre-processing steps can be found on a google drive: dsgelab>Finregistry>Data_dictionaries>QC preprocessing changes. 

# Detailed longitudinal 
Here detailed longitudinal files are formed from all registers which are included in endpoint generation

## Hilmo 

Hilmo registry is split into separate files because throughout the registry existance three ICD disease classification versions changed (8 to 10):
* ICD8: From the start of the register up to the end of 1987
* ICD9: From 1987-01-01 to the end of 1995
* ICD10: from 01-01-1996 up to now

For the period of 1994-1995 there are separate Hilmo files with ICD9 codes (this is due to the cange of coding in 1994 within the register (not due to change in ICD version): "The care notification register was introduced in 1994, in which case it replaced the previously used Deletion Notification Register".

Since 1998 the register also contains outpatient care codes (inpatient and outpatient codes can be distinguished form SOURCE column)

In addition to diagnostic ICD codes, heart surgery codes (recoded from 1994) and other surgical codes are also included in detailed longitudinal. Information on how codes/sources are recorded within detailed longitudinal can be seen in: dsgelab>Finregistry>Data_dictionaries>Data dictionary.xlsx>Detailed longitudinal variables
