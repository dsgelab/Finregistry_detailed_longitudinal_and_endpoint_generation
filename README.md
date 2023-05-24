
This repository contains the script for creating the detailed longitudinal file used in Finregistry, the script have been transposed starting from the original FinnGen one (see here: https://github.com/FINNGEN/service-sector-data-processing/tree/master ) with some minor changes, for more info see **CHANGES** section of this file.

# CODE

*main.py* defines all data file paths and perform all the processing in order to then to create the deatiled longitudial file by concatenating the output files

*func.py* contains all the function used to process the dataset in main.py, for more info see **PREPARATION RULES** section of this file

# CHANGES

- script structure is different from FinnGen because now all processing is performed using registry specific functions
- no adding of PIC to the datasets      --> already available in Finregistry 
- no joining of Finngen IDs             --> already available in Finregistry 
- no removing of ID denials             --> don't have those in Finregistry
- birth date is not created using the htun2date() function but is imported from minimal_phenotype file (Finregistry dataset)
- death date is also imported from minimal_phenotype file (Finregistry dataset)
- in Kela datasets the column names are alreay in uppercase
- in cancer registry not defining the following variables:
'MY_CANC_COD_TOPO','MY_CANC_COD_AGE','MY_CANC_COD_YEAR'

# PROCESSING SUMMARY

Each register is transformed into a detailed longitudinal file structure containing the following variables: <br>FINREGISTRYID, PVM, EVENT_AGE, EVENT_YRMNTH, CODE1, CODE2, CODE3, CODE4, ICDVER, CATEGORY, INDEX, SOURCE. 
\
All missing values in detailed longitudinal are replaced with a string "NA" 
\
NB: EVENT_AGE is going to be round up to 2 decimal positions

## Hilmo 

Hilmo registry is split into separate files because throughout the registry existence three ICD disease classification versions changed (8 to 10):
* ICD8: From the start of the register up to the end of 1986;
* ICD9: From 1987-01-01 to the end of 1995;
* ICD10: From 1996-01-01 up to now.

In addition to that, for the period 1994-1995 there is a separate Hilmo file with ICD9 codes, this is due to the change of coding within the register in 1994 (not due to the change in ICD version): "The care notification register was introduced in 1994, in which case it replaced the previously used Deletion Notification Register".

Since 1998 the register also contains outpatient care codes (inpatient and outpatient codes can be distinguished from the SOURCE column of detailed longitudinal). 
inpatient/outpatient split is made according to 'PALA' up to 2019 and 'YHTEYSTAPA'+’PALA’ codes for 2019-2021. 

In addition to diagnostic ICD codes, heart surgery codes (recorded from 1994) and other surgical codes are also included in the detailed longitudinal. 

Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained a special characters {+,\*,#,@} which were also removed. 

HilmoICD10_heart contains heart surgery codes (recorded from 1994). For a period up to 2019 a small correction is made to a 'CATEGORY' variable which records a source of a code  HPO1:3 - Procedure for demanding heart patient, old coding and HPN1:N - Procedure for demanding heart patient, new coding. Some HPO (old) fully numeric codes were mixed in with HPN (new) codes always starting with the letter “A”. HPN is changed to HPO for fully numeric codes.

## AvoHilmo

Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained a special characters {+,\*,#,@} which were also removed. 

## Cancer Registry

Cancer preprocessing is strigtforward and self-explanatory.

## death registry

six columns contain ICD 8 to 10codes: The basic cause of death (TPKS), the immediate cause of death (VKS)and four contributing causes of death (M1-M4). All those codes are transferred to the ‘CODE1’ column of detailed longitudinal with an appropriate 'CATEGORY' label denoting a code type. 

## Kela purchases

Some light data cleaning was done: 
* removal of entries which did not contain either ATC code or Kela reimbursement code (SAIR)
* removed duplicates 
* death date from COD was used to correct event dates recorded after death date (those dates were changed to death date)

## Kela reimbursement

Kela reimbursement preprocessing is strigtforward and self-explanatory 


# EXTRA INFORMATION

**A note on causes of death pre-processing**

We have received two files "thl2021_2196_ksyy_tutkimus" and " thl2021_2196_ksyy_vuosi ". 
The First larger file (tutkimus) contains information for all IDs from a second file (vuosi) and also contains death dates (but no medical codes) for additional 15467 IDs.  
Trying to match IDs between files there are some small differences in variables (e.g. for 15 IDs different death date is recorded, and for 0.1% IDs cause of death codes differ). As we don’t know which information is correct and differences are only for a small portion of IDs, using only tutkimus seems appropriate.

**A note on the updated Kela reimbursements file**

A data update file despite containing reimbursement information for two additional years (2020-2021) had considerably fewer entries compared to an old file. This in part was due to the removal of rows with missing information (rows with missing full dates were removed in data update file).

