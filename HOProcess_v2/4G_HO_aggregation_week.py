import pandas as pd
import os


def get_filename(date, folder):
    dir = '..\\HOCNAM_week\\HOCNAM\\' + folder + '\\' + date 
    filename = dir
    for root,dirs,files in os.walk(dir):
        for file in files:
            if file.endswith(".csv"):
                filename += '\\' + file
    return filename


FOLDER = 'CSV_ho'  # 'cells', 'CSV_ho' or 'CSV_nbimsi'

DATES = ['2024-10-28',
         '2024-10-29',
         '2024-10-30',
         '2024-10-31',
         '2024-11-01',
         '2024-11-02',
         '2024-11-03']
EXPORT_PATH = 'Processed_data\\4G_'+FOLDER+'_AGG_2024-10-28.snappy.parquet'

print('Aggregating '+FOLDER+' data from '+DATES[0]+' to '+DATES[-1])

week_list = []

#for day in DATES:
for day in ['2024-10-28']:
    print('\tParsing '+day+'...')
    filename = get_filename(day, folder=FOLDER)
    df = pd.read_csv(filename)
    week_list.append(df)
    
print('Starting aggregation...')
frame = pd.concat(week_list, axis=0, ignore_index=True)

if FOLDER=='CSV_ho':
    df_agg = pd.DataFrame(frame.groupby(['prev_eci_4g', 'eci_4g'])[['nb_imsi','nb_ho']].sum())
    df_agg = df_agg[df_agg['nb_ho'] >0] 
    df_agg = df_agg.reset_index()
    df_agg = df_agg.astype(int)
elif FOLDER=='cells':
    df_agg = frame.drop_duplicates(ignore_index=True)
    cells_list = []
    for id, row in df_agg.iterrows():
        prev_name = row['nom_cellule'].split('_')
        cells_list.append('_'.join(prev_name[0:-2]))
    df_agg['Cell'] = cells_list

print('\n... Done. \nAggregated '+FOLDER+' DataFrame:')
print(df_agg)

print('\nExporting DataFrame to '+EXPORT_PATH)
df_agg.to_parquet(path=EXPORT_PATH, engine='fastparquet', compression='snappy')