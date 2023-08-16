#!/bin/bash
###############################################################################
#
# A simple "parallel" script for creating endpoint data from register data for 
# the FinnGen project.
# 
# Author: Andrius Vabalas (andrius.vabalas@helsinki.fi)
# Addapted from: Tero Hiekkalinna (tero.hiekkalinna@thl.fi)
# Copyright (C) The Finnish Institute for Health and Welfare (THL)
#
###############################################################################

# Maximum verbosity
set -x

DEF_FILE="/data/processed_data/endpointer/supporting_files/2020/FINNGEN_ENDPOINTS_DF10_Final_2022-05-16.xlsx"
DEF_CONTROL_FILE="/data/processed_data/endpointer/supporting_files/2020/Controls_FINNGEN_ENDPOINTS_DF10_Final_2022-05-16.xlsx"
BASELINE="/data/processed_data/endpointer/supporting_files/2020/baseline.txt"
MINIMI="/data/processed_data/endpointer/supporting_files/2020/minimi_dummy.txt"
DETAIL_LONG="/data/processed_data/endpointer/supporting_files/2020/longitudinal.txt"
ENDPOINT_SHORT_LIST="/data/processed_data/endpointer/supporting_files/2020/endpoint_short_list.txt"

ID_LIST="/data/processed_data/endpointer/supporting_files/2020/custom_id_list.txt"
# Directory where this file will be run/executed
BASE_DIR="/data/processed_data/endpointer/supporting_files/2020"

split --verbose -a 3 -d -l 10000 $ID_LIST $ID_LIST"."

loop=1
for count in {000..716..30}
do
	DETAIL_LONGITUDINAL=$DETAIL_LONG"."$loop
	a=29
	if [ $count -eq 030 ]; then a=35;  fi
	if [ $count -eq 060 ]; then a=41;  fi
	if [ $count -eq 090 ]; then count=90;  fi
	if [ $count -eq 690 ]; then a=26;  fi
	for INDEX in $(seq -w $count $(($count+$a)))
	do
		echo "$INDEX"

		SUB_DIR="run"$INDEX
		mkdir -p $SUB_DIR
		cd $SUB_DIR

		CUSTOM_LIST=$ID_LIST"."$INDEX
		OUTPUT_FILE="finngen_endpoints.txt"
		OUTPUT_STAT_FILE="finngen_endpoints_stats.txt"
		SCREEN_OUTPUT="out"
		
		nohup python3 $BASE_DIR/finngen_endpointter.py --endpoint-defs $DEF_FILE --endpoint-control-defs $DEF_CONTROL_FILE --baseline $BASELINE --minimi $MINIMI --longitudinal $DETAIL_LONGITUDINAL --endpoint-short-list $ENDPOINT_SHORT_LIST --custom-id-list $CUSTOM_LIST --id-list-source custom --field-separator-longitudinal comma --compress-output --endpoints-output-file $OUTPUT_FILE --endpoints-stats-output-file $OUTPUT_STAT_FILE > $SCREEN_OUTPUT 2>&1 &
		sleep 1s
		cd $BASE_DIR
	done
	sleep 57m
	loop=$(($loop+1))
done
