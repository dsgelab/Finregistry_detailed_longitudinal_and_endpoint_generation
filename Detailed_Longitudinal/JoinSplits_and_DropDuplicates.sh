
InDir=/data/processed_data/detailed_longitudinal/R10/backup/splits_2024_08_30
NewHilmo=$InDir/Hilmo.csv
NewAvohilmo=$InDir/Avohilmo.csv
NewDL=/data/processed_data/detailed_longitudinal/R10/backup/detailed_longitudinal_2024-08-30_NoDuplicates.csv

echo "joining all hilmo splits into $NewHilmo"
start=`date +%s`
head -n 1 $InDir/Hilmo_1969_1986.csv > $NewHilmo
tail -n +2 -q $InDir/Hilmo_* >> $NewHilmo
end=`date +%s`
runtime=$((end-start))
echo "Execution time: $runtime seconds"

echo "joining all avohilmo splits into $NewAvohilmo"
start=`date +%s`
head -n 1 $InDir/Avohilmo_icd10_11_16.csv > $NewAvohilmo
tail -n +2 -q $InDir/Avohilmo_* >> $NewAvohilmo
end=`date +%s`
runtime=$((end-start))
echo "Execution time: $runtime seconds"

echo "removing duplicates from $NewHilmo"
start=`date +%s`
awk -F ',' 'NR==1 || !visited[$1 $2 $3 $4 $5 $6 $7 $8 $9 $10 $11 $12 $13 $14 $15]++ { print $0 }' $NewHilmo > $InDir/HilmoNoDuplicates.csv
end=`date +%s`
runtime=$((end-start))
echo "Execution time: $runtime seconds"

echo "removing duplicates from $NewAvohilmo"
start=`date +%s`
awk -F ',' 'NR==1 || !visited[$1 $2 $3 $4 $5 $6 $7 $8 $9 $10 $11 $12 $13 $14 $15]++ { print $0 }' $NewAvohilmo > $InDir/AvohilmoNoDuplicates.csv
end=`date +%s`
runtime=$((end-start))
echo "Execution time: $runtime seconds"

echo "creating the final version of Detailed Longitudinal"
start=`date +%s`
#prepare header of file
head -n 1 $InDir/Cancer.csv > $NewDL
# Append all files, excluding the header
FileList="HilmoNoDuplicates.csv AvohilmoNoDuplicates.csv Cancer.csv Death.csv KelaPurchase.csv KelaReimbursement.csv"
for file in $FileList; do
    tail -n +2 $InDir/$file >> $NewDL    
done
end=`date +%s`
runtime=$((end-start))
echo "Execution time: $runtime seconds"