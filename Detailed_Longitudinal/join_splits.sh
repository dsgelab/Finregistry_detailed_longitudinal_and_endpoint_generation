
head -n 1 Hilmo_1994_1995.csv > detailed_longitudinal_fromsplits.csv  
for file in /data/processed_data/detailed_longitudinal/R10/service_sector/splits/*; do
    # Append contents, excluding the header
    tail -n +2 "$file" >> detailed_longitudinal_fromsplits.csv    
done