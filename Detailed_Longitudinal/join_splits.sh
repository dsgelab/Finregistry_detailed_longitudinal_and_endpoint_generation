
# define the output directory and the filename to be used
OutDir=/data/processed_data/detailed_longitudinal/R10/backup
DL_FILE=$OutDir/detailed_longitudinal_2024_08_18.csv

#prepare header of file
head -n 1 $OutDir/splits_2024_08_18/Cancer.csv > $DL_FILE  
# Append all files, excluding the header
for file in $OutDir/splits_2024_08_18/*; do
    tail -n +2 "$file" >> $DL_FILE    
done