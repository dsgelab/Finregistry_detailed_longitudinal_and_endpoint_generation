This repository contains scripts for initial registry pre-processing (only registers which are used for endpoint generation), scripts to transform data into a “detailed longitudinal” format and supporting code for running endpointer.py and transforming endpoint files into densified versions.

# Pre-processing
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

Within HilmoICD10_diag file some ICD10 code cleaning was done. Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained special characters '*&#+' which were also removed. 

HilmoICD10_heart contains heart surgery codes (recorded from 1994). For a period up to 2019 a small correction is made to a 'CATEGORY' variable which records a source of a code  HPO1:3 - Procedure for demanding heart patient, old coding and HPN1:N - Procedure for demanding heart patient, new coding. Some HPO (old) fully numeric codes were mixed in with HPN (new) codes always starting with the letter “A”. HPN is changed to HPO for fully numeric codes.

HilmoICD10_oper contains surgical procedure codes

## AvoHilmo

Similarly as with Hilmo preprocessing should start with Main.py as within it, a supporting file is created which contains basic visit information which is then joined with other files containing medical codes (on a unique visit identifier code “AVOHILMO”)

The files for periods 2011-2016, 2017-2020 and 2020-2021 were received and processed separately (due to the large size and limited computational recourses). data for the year 2020 was contained within two files (2017-2020 and 2020-2021) therefore it was only retained from 2020-2021 data update period.

Within ICD diag.py file some ICD10 code cleaning was done. Although nearly all ICD10 codes were recorded without a dot after the initial letter and two first digits, a small portion contained dots which were removed. A small portion of codes contained special characters '*&#+' which were also removed. 

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

# Aggregating/sorting/splitting Detailed longitudinal

To aggregate all detailed longitudinal format files into a single file use bash command: 

```console
awk 'FNR>1 || NR==1' *.csv > all.csv
```

To sort by FINREGISTRYID column and then by EVENT_AGE column use: 

```console
awk -F,  ' { t = $2; $2 = $3; $3 = t; OFS= ","; print; } '  all.csv > all2.csv
sort -T /data/processed_data/detailed_longitudinal/supporting_files/ -t ',' -k1,1n  -S 70G --parallel=30 all2.csv > all3.csv
awk -F,  ' { t = $3; $3 = $2; $2 = t; OFS= ","; print; } '  all3.csv > detailed_longitudinal.csv
```

Then run Utils/split_longitudinal.sh to split detialed longitudinal file containing data for 7166416 IDs (sorted by ID) into 24 files containing 300k IDs.


# Endpoint generation

In this section supporting files are createed and scripts described for running endpointer program [https://github.com/FINNGEN/Endpointter](https://github.com/FINNGEN/Endpointter). Read instructions on Endpointter repository for required input file descriptions.

* Create baseline input file, which contains sex information using Endpointer/baseline.py    
* Create Custom_ID_listinput file using Endpointer/Custom_ID_list.py  
* Use /FINNGEN/Endpointter/test_input/minimi_dummy.txt as another input file (change "FINNGENID" to "FINREGISTRYID" within that file)   
* Use /FINNGENEndpointter/test_input/endpoint_short_list.txt as another input file (make shure that "ALL" is uncommented within that file)  
* Use two MS Excel (xlsx) files with endpoint defintions and control definitions as input files they get piublished in FINNGEN repository e.g. /FINNGEN/DF10-endpoint-and-control-definitions/  (recomend not to use public version as column names in the public controls file are different)   
* Within the main script file /FINNGENEndpointter/finngen_endpointter.py replace all occurances of "FINGENNID" with "FINREGISTRYID"
also comment out rows 6993-6995:

```python
            else:
                if subject_id in self.baseline_data_fu_end_age_map:
                    age = self.baseline_data_fu_end_age_map[subject_id]
```

* Run Endpointer/Parallel_finregistry.sh to generate endpoints (make sure that all paths are correct). The script splits ID list to batches of 10,000 IDs and runs 30 times in parallel (for 300k ID's at a time). This is repeated 24 times in a loop untill endpoints are gnerated for all IDs. A single process uses up to approx 10.0G of memory (for 30 processes running in parallel approx up to 300G of memory needed in total ). Total running time of a single loop is approx 56 minutes or approx 24 hours to generate all endpoints. 

* Run Endpointer/combine_endpoints.sh to combine endpoint longitudinal and wide first events files from separate 716 splits 
* Run Endpointer/densify_first_events2022.py to create densified version of wide first events file, e.g.: 
```console
python3 /data/processed_data/endpointer/supporting_files/2020/densify_first_events2022.py -i /data/processed_data/endpointer/supporting_files/2020/wide_first_events_DF10_2022_09_29.txt.ALL.gz -o /data/processed_data/endpointer/wide_first_events_densified_DF10_2022_09_29.txt
```
