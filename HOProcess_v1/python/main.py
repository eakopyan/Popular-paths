import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import List, Dict

from dataProcessor import parse_handovers, parse_enodeb, fill_coordinates, group_flows, find_eci, check_doubles

gjson_dir_path = "/home/agora/Documents/Handover/geojson/"
analytics_path = "/home/agora/Documents/Popular_paths/Data/Analytics/"
rawcells_path = "/home/agora/Documents/Handover/data/ECI_4G_XY.csv"
cells_path = "/home/agora/Documents/Handover/data/ECI_4G_WGS84.csv"
ho_path="/home/agora/Documents/Handover/data/HO_4G_Lyon.csv"
#agg_ho_path="/home/agora/Documents/Handover/data/HO_4G_AGG.csv"
#agg_ho_path="/home/agora/Documents/Popular_paths/HOProcess_v1/HO_4G_AGG.csv"
agg_ho_path="../data/HO_4G_AGG.csv"


def convert_df():
    df = pd.read_csv(agg_ho_path, sep=';', header=None)
    ho_df = df.drop([0,1]).transpose()
    ho_df = ho_df.rename(columns=ho_df.iloc[0]).drop(ho_df.index[0])
    print('DataFrame size:', len(ho_df.index))
    print(ho_df.head())
    ho_df.to_csv(agg_ho_path, sep=';')
    """ho_df = df.drop(['Unnamed: 0', 'Cell start'], axis=1).astype(int)
    print('DataFrame size:', len(ho_df.index))
    print(ho_df.head())
    ho_df.to_csv(agg_ho_path, sep=';', index=False)"""

def add_coord_end(data, coords):
    data['x1'], data['y1'] = 0.0, 0.0
    with tqdm(desc='Matching end coord', total=len(data.index)) as pbar:
        for idx, row in data.iterrows():
            eci = row['ECI']
            for i, r in coords.iterrows():
                if r['ECI'] == eci: # Matching ECI, hence coordinates
                    data.loc[idx,'x1'], data.loc[idx,'y1'] = r['Longitude'], r['Latitude']
                    break
            pbar.update(1)

def add_coord_start(data):
    data['x2'], data['y2'] = 0.0, 0.0
    visited = {} # EnB ID, coords
    with tqdm(desc='Matching start coord', total=len(data.index)) as pbar:
        for idx, row in data.iterrows():
            enb = row['EnB start']
            if enb in visited:
                (data.loc[idx,'x2'], data.loc[idx,'y2']) = visited[enb]
            else:
                for i, r in data.iterrows():
                    if r['EnB end'] == enb: # Matching EnB ID, hence coordinates
                        data.loc[idx,'x2'], data.loc[idx,'y2'] = r['x1'], r['y1']
                        visited[enb] = (data.loc[idx,'x2'], data.loc[idx,'y2'])
                        break
            pbar.update(1)

def drop_external(data):
    indices = data[data['x2']==0].index
    data.drop(indices, inplace=True)

#===============================================================================
if __name__ == "__main__":
    ho_df = pd.read_csv(agg_ho_path, sep=';')
    print('DataFrame size:', len(ho_df.index))

    #drop_external(ho_df)
    #print('Double check:', len(ho_df.index))

    print(ho_df.head(), '\n')
    print('Minimum weight:', np.amin(ho_df['Weight']))
    print('Q1 weight:', np.quantile(ho_df['Weight'], 0.25))
    print('Median weight:', np.median(ho_df['Weight']))
    print('Q2 weight:', np.quantile(ho_df['Weight'], 0.75))
    print('Maximum weight:', np.amax(ho_df['Weight']))
    print('Mean weight:', np.mean(ho_df['Weight']), '\n')
