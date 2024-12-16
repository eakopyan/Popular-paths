import datetime
import pandas as pd
import glob 
import numpy as np
import time


DAYS = np.arange(16, 23, 1)
MONTH = '03'
PATH_HO = '..\\Data_Handover\\usersHandovers\\2019\\'+MONTH+'\\'
EXPORT_PATH_DAY = 'Processed_data\\HO_AGG_'+str(DAYS[0])+'-'+str(DAYS[-1])+'_DAYTIME.snappy.parquet'
EXPORT_PATH_NIGHT = 'Processed_data\\HO_AGG_'+str(DAYS[0])+'-'+str(DAYS[-1])+'_NIGHTTIME.snappy.parquet'
DIR = '\\Lyon_HO_IN'

print('Aggregating handover data from day '+str(DAYS[0])+' to day '+str(DAYS[-1])+'\t month: '+MONTH)

day_list, night_list = [], []


for day in DAYS:    
    files = glob.glob(PATH_HO + str(day) + DIR + "\\*.parquet")
    
    start_date = datetime.datetime(2019, 3, day, 1, 0, 0)
    end_date = datetime.datetime(2019, 3, day, 6, 0, 0)
    start_utsp = time.mktime(start_date.timetuple())
    end_utsp = time.mktime(end_date.timetuple())
    #print('Nighttime start:', start_date, '\t end:', end_date, 'UTSP start:', start_utsp, '\t end:', end_utsp)

    for filename in files:
        df = pd.read_parquet(filename, engine='fastparquet')
        day_list.append(df.loc[(df.timeSlot < start_utsp) | (df.timeSlot >= end_utsp)])
        night_list.append(df.loc[(df.timeSlot >= start_utsp) & (df.timeSlot < end_utsp)])
        print(len(df), '\t', len(day_list[-1]), '\t', len(night_list[-1]))

# Daytime process
frame_day = pd.concat(day_list, axis=0, ignore_index=True)
df_agg_day = pd.DataFrame(frame_day.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_agg_day = df_agg_day[df_agg_day['nbHO15'] >0] 
print('\nAggregated handover DataFrame (daytime):')
print(df_agg_day)

# Nighttime process
frame_night = pd.concat(night_list, axis=0, ignore_index=True)
df_agg_night = pd.DataFrame(frame_night.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_agg_night = df_agg_night[df_agg_night['nbHO15'] >0] 
print('\nAggregated handover DataFrame (nighttime):')
print(df_agg_night)

# Exports
print('\nExporting daytime DataFrame to '+EXPORT_PATH_DAY)
df_agg_day.to_parquet(path=EXPORT_PATH_DAY, engine='fastparquet', compression='snappy')
print('\nExporting nighttime DataFrame to '+EXPORT_PATH_NIGHT)
df_agg_night.to_parquet(path=EXPORT_PATH_NIGHT, engine='fastparquet', compression='snappy')