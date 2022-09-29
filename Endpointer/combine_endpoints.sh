#!/bin/bash
###############################################################################
#
# A simple script for combining endpoint subset runs into one file
# 
# Any other use of this software outside of FinnGen project requires permission
# from the author.
# 
# Author: Andrius Vabalas (andrius.vabalas@helsinki.fi)
# Addapted from: Tero Hiekkalinna (tero.hiekkalinna@thl.fi)
# 
# Copyright (C) The Finnish Institute for Health and Welfare (THL)
#
###############################################################################

# NOTE: We assume that endpoint files are compressed (.gz extension)!

#
# Version DD-MM-YYYY-[version]
#
VERSION="DF10_2022_09_29"

#
# First event endpoints
#
BIG_FILE="wide_first_events_"$VERSION".txt.ALL.gz"

echo "Master endpoint file: "$BIG_FILE

zcat run000/finngen_endpoints.txt.gz | head -1 | tr -s ' ' '\t' | gzip > $BIG_FILE

#  NOTE: We assume that there are subset runs from 000 to 716

for count in {000..716..30}
do
	a=29
	if [ $count -eq 030 ]; then a=35;  fi
	if [ $count -eq 060 ]; then a=41;  fi
	if [ $count -eq 090 ]; then count=90;  fi
	if [ $count -eq 690 ]; then a=26;  fi
	for INDEX in $(seq -w $count $(($count+$a)))
	do

		FILE="run"$INDEX"/finngen_endpoints.txt.gz"

		echo $FILE

		zcat $FILE | tail -n +2 -q | tr -s ' ' '\t' | gzip >> $BIG_FILE

	done
done



#
# Logitudinal endpoints
#
BIG_LOGITUDINAL_FILE="longitudinal_endpoints_"$VERSION".txt.ALL.gz"

echo "Master longitudinal endpoint file: "$BIG_LOGITUDINAL_FILE

zcat run000/finngen_endpoints_longitudinal.txt.gz | head -1 | tr -s ' ' '\t' | gzip > $BIG_LOGITUDINAL_FILE

#  NOTE: We assume that there are subset runs from 000 to 716

for count in {000..716..30}
do
	a=29
	if [ $count -eq 030 ]; then a=35;  fi
	if [ $count -eq 060 ]; then a=41;  fi
	if [ $count -eq 090 ]; then count=90;  fi
	if [ $count -eq 690 ]; then a=26;  fi
	for INDEX in $(seq -w $count $(($count+$a)))
	do

		FILE="run"$INDEX"/finngen_endpoints_longitudinal.txt.gz"

		echo $FILE

		zcat $FILE | tail -n +2 -q | tr -s ' ' '\t' | gzip >> $BIG_LOGITUDINAL_FILE

	done
done



