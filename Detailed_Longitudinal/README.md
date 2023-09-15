
This repository contains the script for creating the detailed longitudinal file used in Finregistry, the script have been transposed starting from the original FinnGen one (see [here](https://github.com/FINNGEN/service-sector-data-processing/tree/master) ) with some minor changes, for more info see **CHANGES** section of this file.

# CODE

*config.py* contains all data file paths to be used in the creation of the detailed longitudinal file

*main.py* perform all the processing in order to then to create the deatiled longitudial file by concatenating the output files

*func.py* contains all the function used to process the dataset in main.py, for more info see **PROCESSING SUMMARY** section of this file

# CHANGES

- script structure is different from FinnGen because now all processing is performed using registry specific functions
- no adding of PIC to the datasets      --> already available in Finregistry 
- no joining of Finngen IDs             --> already available in Finregistry 
- no removing of ID denials             --> don't have those in Finregistry
- birth date is not created using the htun2date() function but is imported from a sample of minimal_phenotype file (Finregistry dataset) 
- death date is also imported from minimal_phenotype file (Finregistry dataset)
- in Kela datasets the column names are alreay in uppercase
- age randomization is only performed in FinnGen not in Finregistry
- armonize INDEX definition to the one of every other registry

# REGISTRIES TIMELINE

| REGISTRY        | START DATE | END DATE |
|-----------------|------------|----------|
| Hilmo           | jan 1969   | dec 2021 |
| AvoHilmo		  | jan 2011   | dec 2021 |
| Cancer          | jan 1953   | dec 2020 |
| Death           | jan 1971   | dec 2019 |
| Purchases       | jan 1995   | dec 2020 |
| Reimbursements  | jan 1964   | dec 2019 |

# PROCESSING SUMMARY

Each register is transformed into a detailed longitudinal file structure containing the following variables: <br>
FINREGISTRYID, PVM, EVENT_YRMNTH, EVENT_AGE,  
CODE1, CODE2, CODE3, CODE4, CODE5, CODE6, CODE7, 
CATEGORY, INDEX, SOURCE, ICDVER. 

## General Rules

**PROCESSING RULES**:

EVENT_AGE is going to be round up to 2 decimal positions<br>
Check that EVENT_DATE is not after DEATH_DATE

**QUALITY CONTROL RULES**:

If EVENT_AGE is less than 0 or more than 110 then row is deleted<br>
If EVENT_AGE is missing the row is deleted<br>
If CODE1 or CODE2 is missing then row is deleted<br>
If duplicate row then remove row<br>
If CODE4 is negative then set to missing

In Kela dataframes check that the ATC / VNRO code are formatted the correct way

## Hilmo 

Throughout the registry existence three ICD disease classification versions changed (8 to 10):
* ICD8: From the start of the register up to the end of 1986;
* ICD9: From 1987-01-01 to the end of 1995;
* ICD10: From 1996-01-01 up to now.

In addition to that, for the period 1994-1995 there is a separate Hilmo file with ICD9 codes, this is due to the change of coding within the register in 1994 (not due to the change in ICD version): "The care notification register was introduced in 1994, in which case it replaced the previously used Deletion Notification Register".

Finnish Hospital league codes were used until 1996 when the use of Nomesco codes started.

Since 1998 the register also contains outpatient care codes (inpatient and outpatient codes can be distinguished from the SOURCE column of detailed longitudinal). The inpatient/outpatient split is made according to 'PALA' from 1998 to 2019 and 'YHTEYSTAPA'+’PALA’ codes from 2019 to 2021 (now).   
NB: see functions *Define_INPAT()*,*Define_OPERIN()* and *Define_OPEROUT()* in **func.py** 

In addition to the hilmo inpatient and outpatient files we have information about:
- diagnostic ICD codes (hilmo diagnosis) to be joined to hilmo after 1995, before that year the codes where already present in the main hilmo dataset.
- heart surgery codes (hilmo heart, recorded from 1994)
- other surgical codes (hilmo operations) referring to day hospital operations. 

For a period up to 2019 a small correction is made inside hilmo heart to the 'CATEGORY' variable which records a source of a code.

HPO1:3 - Procedure for demanding heart patient, old coding and <br>
HPN1:N - Procedure for demanding heart patient, new coding. 

Some HPO (old) fully numeric codes were mixed in with HPN (new) codes always starting with the letter “A”. HPN is changed to HPO for fully numeric codes.

Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained a special characters {+,\*,#,@} which were also processed.
For more info check the function *CombinationCodesSplit()* in **func.py** 

NB: for quality control reasons, the CATEGORY value for ICD codes will be limited between 0 and 3, where 0 is going to represent the main diagnosis and 1,2,3 are going to be side diagnosis


## AvoHilmo

Register codes given in the primary health care visits are not as confirmed as codes given in hospital (inpatient data) or codes coming from the specialized outpatient visits (outpatient data). Finnish doctors are legally responsible for ICD codes in Hilmo, but Avohilmo codes do not carry the same responsibility. Avohilmo data includes also codes that are given by nurse (ICPC2), these codes include procedures as well.

Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained a special characters {+,\*,#,@} which were also removed. 

In addition to icd10 and icpc2 information, there are files (to be joined with avohilmo) referring to dental measures and interventions.

## Cancer Registry

Processing is strigtforward and self-explanatory, nothing to declare.

## death registry

Between 1969 to 1986, the international classification ICD-8 was in use, with Finnish additions. Some Finnish additions to the ICD-codes can be found from [here](https://taika.stat.fi/en/aineistokuvaus.html#!?dataid=ksyyt_197100_jua_kuolemansyyt_001.xml).

Between 1987 and 1995, the data were classified using the national classification of diseases ICD9, where comparability to international version is maintained.

Since 1996, the statistics have been compiled based on the 10th revision of the International Classification of Diseases (ICD-10), with some Finnish additions (listed [here](https://taika.stat.fi/en/aineistokuvaus.html#!?dataid=ksyyt_197100_jua_kuolemansyyt_001.xml) ). 
Note that this is the WHO version of ICD10 and therefore does not include some of the subtypes/extensions of the ICD10 codes used in the full Finnish se of ICD codes.

There are six columns contain ICD codes: 
The basic cause of death (TPKS), the immediate cause of death (VKS) and four contributing causes of death (M1-M4). All those codes are transferred to the ‘CODE1’ column of detailed longitudinal with an appropriate 'CATEGORY' label denoting a code type. 

## Kela purchases


Some light data cleaning was done: 
* removal of entries which did not contain either ATC code or Kela reimbursement code (SAIR)
* removed duplicates 
* all VNRO codes to be without "." and 6-digit long 
* death date from COD was used to correct event dates recorded after death date (those dates were changed to death date)

## Kela reimbursement

Processing is strigtforward and self-explanatory, nothing to declare.

# EXTRA INFORMATION

**FinnGen handbook**

look at this file for more info on the registers used for detailed longitudinal
https://finngen.gitbook.io/finngen-analyst-handbook/finngen-data-specifics/red-library-data-individual-level-data/what-phenotype-files-are-available-in-sandbox-1/detailed-longitudinal-data/registers-in-the-detailed-longitudinal-data

and this for more info on the CODEs names/meaning
https://finngen.gitbook.io/finngen-analyst-handbook/finngen-data-specifics/finnish-health-registers-and-medical-coding/international-and-finnish-health-code-sets


**A note on causes of death pre-processing**

We have received two files "thl2021_2196_ksyy_tutkimus" and " thl2021_2196_ksyy_vuosi ". 
The First larger file (tutkimus) contains information for all IDs from a second file (vuosi) and also contains death dates (but no medical codes) for additional 15467 IDs.  
Trying to match IDs between files there are some small differences in variables (e.g. for 15 IDs different death date is recorded, and for 0.1% IDs cause of death codes differ). As we don’t know which information is correct and differences are only for a small portion of IDs, using only tutkimus seems appropriate.

**A note on the updated Kela reimbursements file**

A data update file despite containing reimbursement information for two additional years (2020-2021) had considerably fewer entries compared to an old file. This in part was due to the removal of rows with missing information (rows with missing full dates were removed in data update file).

Also column names have been changed in the meanwhile

