import datetime
import pandas as pd
import glob 
import numpy as np
import time

"""

This file aggregates data as follows:
- night (00h-6h)
- morning (6h-12h)
- day (12h-18h)
- evening (18h-00h)
With a separation between week days (18-22/03/2019) and weekend (16-17/03/2019).

Generated files:
- HO_AGG_WEEKDAY : week days with mention of NMDE in the "Time" column
- HO_AGG_WEEKEND : weekend with mention of NMDE in the "Time" column

"""


MONTH = '03'
PATH_HO = '..\\Data_Handover\\usersHandovers\\2019\\'+MONTH+'\\'
DIR = '\\Lyon_HO_IN'



#====================================== WEEK DAYS ===========================================
DAYS = [18,19,20,21,22]
print('Aggregating handover data from day '+str(DAYS[0])+' to day '+str(DAYS[-1])+'\t month: '+MONTH)

# Concatenation
list_night, list_morning, list_day, list_evening = [], [], [], []

for day in DAYS:    
    #hour_night = datetime.datetime(2019, 3, day, 0, 0, 0)
    hour_morning = datetime.datetime(2019, 3, day, 6, 0, 0)
    hour_day = datetime.datetime(2019, 3, day, 12, 0, 0)
    hour_evening = datetime.datetime(2019, 3, day, 18, 0, 0)
    
    #utsp_night = time.mktime(hour_night.timetuple())
    utsp_morning = time.mktime(hour_morning.timetuple())
    utsp_day = time.mktime(hour_day.timetuple())
    utsp_evening = time.mktime(hour_evening.timetuple())
    #print('Nighttime start:', start_date, '\t end:', end_date, 'UTSP start:', start_utsp, '\t end:', end_utsp)

    files = glob.glob(PATH_HO + str(day) + DIR + "\\*.parquet")
    for filename in files:
        df = pd.read_parquet(filename, engine='fastparquet')
        list_night.append(df.loc[df.timeSlot < utsp_morning])
        list_morning.append(df.loc[(df.timeSlot >= utsp_morning) & (df.timeSlot < utsp_day)])
        list_day.append(df.loc[(df.timeSlot >= utsp_day) & (df.timeSlot < utsp_evening)])
        list_evening.append(df.loc[df.timeSlot >= utsp_evening])


# Aggregation
frame_night = pd.concat(list_night, axis=0, ignore_index=True)
frame_morning = pd.concat(list_morning, axis=0, ignore_index=True)
frame_day = pd.concat(list_day, axis=0, ignore_index=True)
frame_evening = pd.concat(list_evening, axis=0, ignore_index=True)
df_night = pd.DataFrame(frame_night.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_night['Time'] = 'NIGHT'
df_morning = pd.DataFrame(frame_morning.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_morning['Time'] = 'MORNING'
df_day = pd.DataFrame(frame_day.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_day['Time'] = 'DAY'
df_evening = pd.DataFrame(frame_evening.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_evening['Time'] = 'EVENING'

df_agg = pd.concat([df_night, df_morning, df_day, df_evening], axis=0)
df_agg = df_agg[df_agg['nbHO15'] >0] 
print('\nAggregated handover DataFrame (week days):')
print(df_agg)

# Export
EXPORT_PATH = 'Processed_data\\HO_AGG_'+str(DAYS[0])+'-'+str(DAYS[-1])+'_WEEKDAY.snappy.parquet'
print('\nExporting week days DataFrame to '+EXPORT_PATH)
df_agg.to_parquet(path=EXPORT_PATH, engine='fastparquet', compression='snappy')



#====================================== WEEKEND ===========================================
DAYS = [16,17]
print('\nAggregating handover data from day '+str(DAYS[0])+' to day '+str(DAYS[-1])+'\t month: '+MONTH)

# Concatenation
list_night, list_morning, list_day, list_evening = [], [], [], []

for day in DAYS:    
    #hour_night = datetime.datetime(2019, 3, day, 0, 0, 0)
    hour_morning = datetime.datetime(2019, 3, day, 6, 0, 0)
    hour_day = datetime.datetime(2019, 3, day, 12, 0, 0)
    hour_evening = datetime.datetime(2019, 3, day, 18, 0, 0)
    
    #utsp_night = time.mktime(hour_night.timetuple())
    utsp_morning = time.mktime(hour_morning.timetuple())
    utsp_day = time.mktime(hour_day.timetuple())
    utsp_evening = time.mktime(hour_evening.timetuple())
    #print('Nighttime start:', start_date, '\t end:', end_date, 'UTSP start:', start_utsp, '\t end:', end_utsp)

    files = glob.glob(PATH_HO + str(day) + DIR + "\\*.parquet")
    for filename in files:
        df = pd.read_parquet(filename, engine='fastparquet')
        list_night.append(df.loc[df.timeSlot < utsp_morning])
        list_morning.append(df.loc[(df.timeSlot >= utsp_morning) & (df.timeSlot < utsp_day)])
        list_day.append(df.loc[(df.timeSlot >= utsp_day) & (df.timeSlot < utsp_evening)])
        list_evening.append(df.loc[df.timeSlot >= utsp_evening])


# Aggregation
frame_night = pd.concat(list_night, axis=0, ignore_index=True)
frame_morning = pd.concat(list_morning, axis=0, ignore_index=True)
frame_day = pd.concat(list_day, axis=0, ignore_index=True)
frame_evening = pd.concat(list_evening, axis=0, ignore_index=True)
df_night = pd.DataFrame(frame_night.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_night['Time'] = 'NIGHT'
df_morning = pd.DataFrame(frame_morning.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_morning['Time'] = 'MORNING'
df_day = pd.DataFrame(frame_day.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_day['Time'] = 'DAY'
df_evening = pd.DataFrame(frame_evening.groupby(['srcLocInfo', 'dstLocInfo'])['nbHO15'].sum())
df_evening['Time'] = 'EVENING'

df_agg = pd.concat([df_night, df_morning, df_day, df_evening], axis=0)
df_agg = df_agg[df_agg['nbHO15'] >0] 
print('\nAggregated handover DataFrame (weekend):')
print(df_agg)

# Export
EXPORT_PATH = 'Processed_data\\HO_AGG_'+str(DAYS[0])+'-'+str(DAYS[-1])+'_WEEKEND.snappy.parquet'
print('\nExporting week days DataFrame to '+EXPORT_PATH)
df_agg.to_parquet(path=EXPORT_PATH, engine='fastparquet', compression='snappy')
