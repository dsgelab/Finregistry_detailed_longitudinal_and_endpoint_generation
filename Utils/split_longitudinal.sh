#!/bin/bash
###############################################################################
#
# A simple script for spliting detailed longitudinal file (sorted by ID) to 
# chunks containing 300000 IDs each for the FinnGen project.
# 
# A file of 7166416 IDs is splt into 24 files
# 
# Author: Andrius Vabalas (andrius.vabalas@helsinki.fi)
#
###############################################################################
set -x

DETAIL_LONGITUDINAL="/data/processed_data/endpointer/supporting_files/2020/detailed_longitudinal.csv"

loop=1
line=1
for count in {1..24..1} #for count in {0000001..7166416..1000}
do
	echo "$loop"
	a=$((count*300000+1))
	if [ $count -gt 3 ]; then b="FR"; else b="FR0"; fi
	ID="${b}${a}"
	echo "$ID"
	if [ $count -eq 24 ]
	then
		patt=$line",$ p"
	else
		z=$(grep -n -o -m 1 $ID $DETAIL_LONGITUDINAL | cut -f1 -d:)
		line1=$(($z-1))
		patt=$line","$line1"p;"$line1"q"
	fi
	fname="longitudinal.txt."$loop
	if [ $count -eq 1 ]
	then
		sed -n -e "$patt" "$DETAIL_LONGITUDINAL" > "$fname"
	else
		sed -n -e '1p' -e "$patt" "$DETAIL_LONGITUDINAL" > "$fname"
	fi
	line=$z
	loop=$(($loop+1)) 
done

