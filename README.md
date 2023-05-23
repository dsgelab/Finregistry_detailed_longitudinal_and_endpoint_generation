
This repository contains the script for creating the detailed longitudinal file used in Finregistry, the script have been transposed starting from the original FinnGen one (see here: ) with some minor changes, for more info see **CHANGES** section of this file.

# CODE

*main.py* is used to create the deatiled longitudial file
*func.py* contains all the function used to prepare the dataset that are then going to be merged in the detailed longitudinal file, for more info see **PREPARATION RULES** section of this file


# PROCESSING SUMMARY

NB: EVENT_AGE is going to be round up to 2 decimal positions

**69-86 hilmo**: 
- import data and DOB
- format date columns 

- evaluate EVENT_AGE
- evaluate EVENT_YEARMTH

- insert INDEX
- SOURCE = 'INPAT'
- ICDVER = 8

- create CODE 1 + CATEGORY by going from wide to long script on the desired columns
DESIRED COLUMNS: 'DG1','DG2','DG3','DG4'
- CODE 2-9 are missing 


**87-93 hilmo**: 
- import data and DOB
- format date columns 

- evaluate EVENT_AGE
- evaluate EVENT_YEARMTH

- insert INDEX
- SOURCE = 'INPAT'
- ICDVER = 9

- create CODE 1 + CATEGORY by going from wide to long script on the desired columns
DESIRED COLUMNS: 'PDG','SDG1','SDG2','SDG3'
- CODE 2-9 are missing 

**94-95 hilmo**: 
- import data and DOB
- format date columns 

- evaluate EVENT_AGE
- evaluate EVENT_YEARMTH

- insert INDEX
- SOURCE = 'INPAT'
- ICDVER = 9

- create CODE 1 + CATEGORY going from wide to long script on the desired columns
DESIRED COLUMNS: 
- CODE 2-9 are missing 


**hilmo diagnosis**

**hilmo operations**

**hilmo heart**

**Death Registry**
- import data and DOB
- format date columns 

- evaluate EVENT_AGE
- evaluate EVENT_YEAR
- evaluate EVENT_YEARMTH

- insert INDEX
- SOURCE = 'DEATH'
- ICDVER = calculated based on EVENT_YEAR

- remove rows with missing EVENT_AGE 
- remove rows with missing CODE1 and CODE2


**Cancer Registry**
- import data and DOB
- format date columns 

- evaluate EVENT_AGE
- evaluate EVENT_YEAR
- evaluate EVENT_YEARMTH

- insert INDEX
- SOURCE = 'CANC'
- ICDVER = calculated based on EVENT_YEAR

- remove rows with missing EVENT_AGE 
- remove rows with missing CODE1 and CODE2


**Kela Reimbursement**

- import data and DOB
- format date columns 

- evaluate EVENT_AGE
- evaluate EVENT_YEAR
- evaluate EVENT_YEARMTH

- insert INDEX
- SOURCE = 'REIMB'
- ICDVER = calculated based on EVENT_YEAR

- remove rows with missing EVENT_AGE 
- remove rows with missing CODE1 and CODE2
- remove duplicates



**Kela Purchases**
- import data and DOB
- format date columns 

- evaluate EVENT_AGE
- evaluate EVENT_YEAR
- evaluate EVENT_YEARMTH

- insert INDEX
- SOURCE = 'PURCH'
- ICDVER = calculated based on EVENT_YEAR

- remove rows with missing EVENT_AGE 
- remove rows with missing CODE1 and CODE2
- remove duplicates



# CHANGES

- no adding of PIC to the datasets      --> already available in Finregistry 
- no joining of Finngen IDs             --> already available in Finregistry 
- no removing of ID denials             --> don't have those in Finregistry

- birth date is not created using the htun2date() function but is imported 
- in Kela datasets the column names are alreay in uoppercase



# EXTRA INFORMATION (Andrius part)
Small pre-processing steps (e.g. date format harmonisation, duplicate removal is performed here). Neither number of columns, nor column names are changed from the original.
All pre-processing steps can be found on google drive: dsgelab>Finregistry>Data_dictionaries>QC preprocessing changes. 

A note on causes of death pre-processing:

We have received two files "thl2021_2196_ksyy_tutkimus" and " thl2021_2196_ksyy_vuosi ". The First larger file (tutkimus) contains information for all IDs from a second file (vuosi) and also contains death dates but no medical codes for additional 15467 IDs.  For matching IDs between files there are some small differences in variables (e.g. for 15 IDs different death date is recorded, and for 0.1% IDs cause of death codes differ). As we don’t know which information is correct and differences are only for a small portion of IDs, using only tutkimus seems appropriate.

A note on the updated Kela reimbursements file: 

A data update file despite containing reimbursement information for two additional years (2020-2021) had considerably fewer entries compared to an old file. This in part was due to the removal of rows with missing information (rows with missing full dates were removed in data update file).

For Kela purchases there was a separate file for each month of the year (12 files per year) for the years 2020 and 2021. During processing 12 files per year were aggregated into a single file per year. 

# Detailed longitudinal pre-processing

Here detailed longitudinal files are formed from all registers which are included in endpoint generation.

First Causes of death (COD) should be processed as the death date from COD is used to correct events recorded after the death date in other registers. The order of processing other registers is not important. 

For how and why certain detailed longitudinal file variables are formatted or what information is included see: google drive: dsgelab>Finregistry>Data_dictionaries>Data Dictionary> Detailed longitudinal variables.

Each register is transformed into a detailed longitudinal format containing variables: FINREGISTRYID, SOURCE, EVENT_AGE, PVM, EVENT_YRMNTH, CODE1, CODE2, CODE3, CODE4, ICDVER, CATEGORY, INDEX. Most steps are self-explanatory or are clear from Data Dictionary> Detailed longitudinal variables sheet, therefore below only register-specific processing steps are given.

All missing values in detailed longitudinal are replaced with a string "NA", this is required by the endpointer.py script for endpoint generation. 

##  Causes of death (COD)

In this register, six columns contain ICD 8 to 10 codes: The basic cause of death, the immediate cause of death and four contributing causes of death. All those codes are transferred to the ‘CODE1’ column of detailed longitudinal with an appropriate 'CATEGORY' label denoting a code type. 
In addition, processed file is saved separately to get the death date to correct events recorded after the death date in other registers.


## Hilmo 

Hilmo registry is split into separate files because throughout the registry existence three ICD disease classification versions changed (8 to 10):
* ICD8: From the start of the register up to the end of 1986;
* ICD9: From 1987-01-01 to the end of 1995;
* ICD10: From 1996-01-01 up to now.

In addition to that, for a period of 1994-1995, there are separate Hilmo files with ICD9 codes, this is due to the change of coding within the register in 1994 (not due to the change in ICD version): "The care notification register was introduced in 1994, in which case it replaced the previously used Deletion Notification Register".

Since 1998 the register also contains outpatient care codes (inpatient and outpatient codes can be distinguished from the SOURCE column).

In addition to diagnostic ICD codes, heart surgery codes (recorded from 1994) and other surgical codes are also included in the detailed longitudinal. Information on how codes/sources are recorded within the detailed longitudinal can be seen in: dsgelab>Finregistry>Data_dictionaries>Data dictionary.xlsx>Detailed longitudinal variables.

There are main registry files covering the period of up to 2019 received in 2021 and update files for a period from 2019-2021 received in 2022. From the main files the codes up to the end of 2018 were used and from updated files the codes from 2019 onwards were used.

The order of processing Hilmo files before ICD10 (HilmoICD89 and Hilmo_Oper89) Is not important. HilmoICD89 contains ICD 8 and 9 codes from three separate register files for the periods: 94-95 (ICD 9), 87-93 (ICD 9) and 69-86 (ICD 8). Hilmo_Oper89 contains surgery codes also from three separate register files for the same periods.

For ICD10 period processing should start with HilmoICD10_main as within it, a supporting file is created which contains basic visit information which is then joined with other files containing medical codes (on a unique visit identifier code “HILMO_ID”) to produce detailed longitudinal format files. In a main file inpatient/outpatient split is also made according to 'PALA' up to 2019 and 'YHTEYSTAPA'+’PALA’ codes for 2019-2021. 

Within HilmoICD10_diag file some ICD10 code cleaning was done. Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained special characters '\*&#+' which were also removed. 

HilmoICD10_heart contains heart surgery codes (recorded from 1994). For a period up to 2019 a small correction is made to a 'CATEGORY' variable which records a source of a code  HPO1:3 - Procedure for demanding heart patient, old coding and HPN1:N - Procedure for demanding heart patient, new coding. Some HPO (old) fully numeric codes were mixed in with HPN (new) codes always starting with the letter “A”. HPN is changed to HPO for fully numeric codes.

HilmoICD10_oper contains surgical procedure codes

## AvoHilmo

Similarly as with Hilmo preprocessing should start with Main.py as within it, a supporting file is created which contains basic visit information which is then joined with other files containing medical codes (on a unique visit identifier code “AVOHILMO”)

The files for periods 2011-2016, 2017-2020 and 2020-2021 were received and processed separately (due to the large size and limited computational recourses). data for the year 2020 was contained within two files (2017-2020 and 2020-2021) therefore it was only retained from 2020-2021 data update period.

Within ICD diag.py file some ICD10 code cleaning was done. Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained special characters '\*&#+' which were also removed. 

ICPC2.py file contains ICPC2 codes

Oper.py file contains operation codes

Mouth.py contains dental procedure codes


## Cancer

Cancer preprocessing is strigtforward and self-explanatory (looking at the code and data dictionary (detailed longitudinal variables sheet)).

## Kela purchases

Some light data cleaning was done during the creation of detailed longitudinal files form purchase information which was recorded in dsgelab>Finregistry>Data_dictionaries>QC preprocessing changes (Detailed longitudinal QC v2 sheet). 

These cleaning steps were: 
* removal of entries which did not contain either ATC code or Kela reimbursement code (SAIR)
* removed duplicates 
* death date from COD was used to correct event dates recorded after death date (those dates were changed to death date)
* a check was prformed whether there were any event dates recorded before the birth date

## Kela reimbursement

Kela reimbursement preprocessing is strigtforward and self-explanatory (looking at the code and data dictionary (detailed longitudinal variables sheet)).
