"""
Remove OMITED endpoints from endpoint longitudinal file

The orignal logitudianl-format has a lot of endpoints
which are marked as omits in endpoint definition file.
'OMIT'==2 and 'OMIT'==1 are exluded, except DEATH endpoint
and 'Modification_reason'=='EXMORE/EXALLC priorities' endpoints

Note that this script doesn't do any input validation. It's also why it's quite fast.


Usage
-----
  python remove_OMITs_longitudinal.py --help


Input file
----------
- First events
  First-event file with a matrix-like structure:
  . columns: endpoints with additional columns for age, year, number of events
  . rows: one individual per row
  Source: FinnGen data


Output
------
Ouputs to stdout by default, use redirection to put the result in a file.
- Dense first events
  CSV format
  . columns: individual FinnGen ID, endpoint, age, year, number of events
  . rows: one row per event, so an individual's events span multiple rows

example command to run script: 
python3 /data/processed_data/endpointer/supporting_files/2020/remove_OMITs_longitudinal2022.py --input /data/processed_data/endpointer/longitudinal_endpoints_DF10_2022_09_29.txt.ALL.gz --output /data/processed_data/endpointer/longitudinal_endpoints_no_omits_DF10_2022_09_29.txt.ALL.gz

"""

import argparse
from pathlib import Path
import pandas as pd
import time
import gzip

def cli_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input_",
        help="path to the longitudinal endpointer file (CSV)",
        required=True,
        type=Path
    )
    parser.add_argument(
        "-o", "--output",
        help="path to output 'with removed OMIT endpoints' file (CSV)",
        required=True,
        type=Path
    )
    args = parser.parse_args()
    return args


def main():
    args = cli_parser()

    out_file = gzip.open(args.output, 'wt')
    in_file = gzip.open(args.input_, 'rt')

    cont = pd.read_excel('/data/processed_data/endpointer/supporting_files/2020/Controls_FINNGEN_ENDPOINTS_DF10_Final_2022-05-16.xlsx')
    defi = pd.read_excel('/data/processed_data/endpointer/supporting_files/2020/FINNGEN_ENDPOINTS_DF10_Final_2022-05-16.xlsx')
    endpoints = cont[cont['OMIT'].isna()]['NAME'].unique().tolist()
    adverse = defi[defi['HD_ICD_10_ATC']=='ANY']['NAME'].values.tolist()
    endpoints=[x for x in endpoints if x not in adverse]
    endpoints.remove('F5_SAD')
    
    # make header
    OUT_HEADER = in_file.readline()
    OUT_HEADER = ",".join(OUT_HEADER.rstrip("\n").split())
    print(OUT_HEADER, file=out_file)
    
    # write only not omited endpoints to an output file
    for row in in_file:
        records = row.rstrip("\n").split()
        if records[5] not in endpoints:
            continue
        else:
            print(
                ",".join(records),
                file=out_file
            )
    in_file.close()
    out_file.close()

if __name__ == "__main__":
    main()
