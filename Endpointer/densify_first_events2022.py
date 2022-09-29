"""
Transform the wide-format first-event file into a long-format / "dense".

The orignal wide-format first-event file has a huge amount of
columns. Some tools like Pandas or SQLite don't deal really well with
that, so we transform the data to make this easier.
We keep only the data if an individual has an endpoint.

Note that this script doesn't do any input validation. It's also why it's quite fast.


Usage
-----
  python densify_first_events.py --help


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

"""

import argparse
from pathlib import Path
import gzip
import pandas as pd

# How the controls, cases, and excluded controls are coded in the input file
CONTROL      = "0"
CASE         = "1"
EXCL_CONTROL = "NA"

# Headers of the output file
OUT_HEADER = ",".join([
    "FINNGENID",
    "ENDPOINT",
    # Input notation:  control=0, case=1, excluded control=NA
    # Output notation: control=1, case=1, excluded control=2
    #"CONTROL_CASE_EXCL",
    "AGE",
    #"YEAR",
    "NEVT"
])


def cli_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input-first-events",
        help="path to the FinnGen first-event phenotype file (CSV)",
        required=True,
        type=Path
    )
    parser.add_argument(
        "-o", "--output",
        help="path to output 'densified' file (CSV)",
        required=True,
        type=Path
    )
    parser.add_argument(
        "-k", "--keep-all",
        help="keep all of controls, excluded controls, and cases, instead of keeping only the cases",
        required=False,
        action="store_true"
    )
    args = parser.parse_args()
    return args


def main():
    args = cli_parser()

    out_file = open(args.output, "x")
    in_file = gzip.open(args.input_first_events, 'rt')

    # Read the header to build a lookup table for column -> column index
    in_header = {}
    for idx, col in enumerate(
            in_file.readline()
            .rstrip("\n")
            .split()):
        in_header[col] = idx

    cont = pd.read_excel('/data/processed_data/endpointer/supporting_files/2020/Controls_FINNGEN_ENDPOINTS_DF10_Final_2022-05-16.xlsx')
    defi = pd.read_excel('/data/processed_data/endpointer/supporting_files/2020/FINNGEN_ENDPOINTS_DF10_Final_2022-05-16.xlsx')
    endpoints = cont[cont['OMIT'].isna()]['NAME'].unique().tolist()
    adverse = defi[defi['HD_ICD_10_ATC']=='ANY']['NAME'].values.tolist()
    endpoints=[x for x in endpoints if x not in adverse]
    endpoints.remove('F5_SAD')


    # Add headers to output file
    print(OUT_HEADER, file=out_file)

    # Get the endpoint data for each individual
    for row in in_file:
        records = row.rstrip("\n").split()

        for endp in endpoints:
            col = in_header[endp]
            val_endp = records[col]
            val_fgid = records[0]

            # Check if case, control or excluded control
            if val_endp not in (CONTROL, CASE, EXCL_CONTROL):
                raise ValueError(f"Unexpected value `{val_endp}` for `{val_fgid}` with endpoint `{endp}` .")
            elif val_endp == EXCL_CONTROL:
                kind = "2"
            else:
                kind = val_endp

            # Get the event info
            if args.keep_all or kind == CASE:
                col_age = in_header[endp + "_AGE"]
                col_nevt = in_header[endp + "_NEVT"]
                val_age = records[col_age]
                val_nevt = records[col_nevt]

                print(
                    f"{val_fgid},{endp},{val_age},{val_nevt}",
                    file=out_file
                )

    in_file.close()
    out_file.close()


if __name__ == "__main__":
    main()
