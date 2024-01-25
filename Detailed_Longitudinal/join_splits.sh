
DL_FILE=/data/processed_data/detailed_longitudinal/R10/service_sector/detailed_longitudinal_2024_19_01.csv
head -n 1 /data/processed_data/detailed_longitudinal/R10/service_sector/splits/Cancer.csv > $DL_FILE  
for file in /data/processed_data/detailed_longitudinal/R10/service_sector/splits/*; do
    # Append contents, excluding the header
    tail -n +2 "$file" >> $DL_FILE    
done
