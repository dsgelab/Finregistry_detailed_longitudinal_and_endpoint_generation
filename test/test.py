
# NB: when running this in epouta machine add this:

# import sys
# sys.path.append('/service_sector_update/')

from func import *
from config import *

# import info on Date_Of_Birth and Date_Of_Death 
minimal_pheno = pd.read_csv('/data/processed_data/minimal_phenotype/minimal_phenotype_2023-05-02.csv',sep = ',', encoding='latin-1')
BIRTH_DEATH_MAP  = min_pheno[:,['FINREGISTRYID','date_of_birth','death_date']]

# run the desired function on desired file (remember test=True)
Hilmo_69_86_processing(file_path=Hilmo_69_86, DOB_map=BIRTH_DEATH_MAP, test=True)
