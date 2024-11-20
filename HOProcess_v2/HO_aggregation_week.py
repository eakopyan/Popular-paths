import pandas as pd
import glob 
import numpy as np


DAYS = np.arange(16, 23, 1)
MONTH = '03'
PATH_HO = '..\\Data_Handover\\usersHandovers\\2019\\'+MONTH+'\\'
EXPORT_PATH = 'Processed_data\\HO_AGG_'+str(DAYS[0])+'-'+str(DAYS[-1])+'.snappy.parquet'
DIR = '\\Lyon_HO_IN'

print('Aggregating handover data from day '+str(DAYS[0])+' to day '+str(DAYS[-1])+'\t month: '+MONTH)

week_list = []

for day in DAYS:
    files = glob.glob(PATH_HO + str(day) + DIR + "\\*.parquet")

    for filename in files:
        df = pd.read_parquet(filename, engine='fastparquet')
        week_list.append(df)

frame = pd.concat(week_list, axis=0, ignore_index=True)


df_agg = pd.DataFrame(frame.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_agg = df_agg[df_agg['nbHO15'] >0] 

print('... Done. \nAggregated handover DataFrame:')
print(df_agg)

print('\nExporting DataFrame to '+EXPORT_PATH)
df_agg.to_parquet(path=EXPORT_PATH, engine='fastparquet', compression='snappy')