This repository contains scripts for initial registry pre-processing (only registers which are used for endpoint generation), scripts to transform data into “detailed longitudinal” format and supporting code for running endpointer.py and transforming endpoint files into densified versions.

# Pre-processing
Small pre-processing steps (e.g. date format harmonisation, duplicate removal is performed here). Neither number of columns, nor column names are changed from original.
All pre-processing steps can be found on a google drive: dsgelab>Finregistry>Data_dictionaries>QC preprocessing changes. 

A note on causes of death pre-processing:

We have received two files "thl2021_2196_ksyy_tutkimus" and " thl2021_2196_ksyy_vuosi ". First larger file (tutkimus) contains information for all IDs from a second file (vuosi) and also contains death date but no medical codes for additional 15467 IDs.  For matching IDs between files there are some small differences in variables (e.g. for 15 IDs different death date is recorded, for 0.1% IDs cause of death codes differ). As we don’t know which information is correct and differences are only for a small portion of IDs, using only tutkimus seems appropriate.

# Detailed longitudinal 
Here detailed longitudinal files are formed from all registers which are included in endpoint generation.

First Causes of death (COD) should be processed as death date from COD is used to correct events recorded after death date in other registers. The order of processing other registers is not important. 

For how and why certain detailed longitudinal file variables are formatted or what information is included see: google drive: dsgelab>Finregistry>Data_dictionaries>Data Dictionary> Detailed longitudinal variables.

##  Causes of death (COD)



## Hilmo 

Hilmo registry is split into separate files because throughout the registry existance three ICD disease classification versions changed (8 to 10):
* ICD8: From the start of the register up to the end of 1986;
* ICD9: From 1987-01-01 to the end of 1995;
* ICD10: From 1996-01-01 up to now.

In addition to that, for a period of 1994-1995 there are separate Hilmo files with ICD9 codes, this is due to the change of coding within the register in 1994 (not due to the change in ICD version): "The care notification register was introduced in 1994, in which case it replaced the previously used Deletion Notification Register".

Since 1998 the register also contains outpatient care codes (inpatient and outpatient codes can be distinguished from the SOURCE column).

In addition to diagnostic ICD codes, heart surgery codes (recorded from 1994) and other surgical codes are also included in detailed longitudinal. Information on how codes/sources are recorded within the detailed longitudinal can be seen in: dsgelab>Finregistry>Data_dictionaries>Data dictionary.xlsx>Detailed longitudinal variables.

There are main registry files covering the period of up to 2019 received in 2021 and update files for a period from 2019-2021 received in 2022. From the main files the codes up to the end of 2018 were used and from an updated files the codes from 2019 onwards were used.
