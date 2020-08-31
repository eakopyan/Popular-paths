import folium
import numpy as np
import pandas as pd
import geopandas as gpd

realdata_path = '/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/GeoJson/{}'.format
rawdata_path = '/home/agora/Documents/Popular_paths/Data/data/{}'.format

def get_stats(data, column: str):
    stats = {}
    stats['min'] = np.amin(data[column])
    stats['q1'] = np.quantile(data[column], 0.25)
    stats['med'] = np.median(data[column])
    stats['q2'] = np.quantile(data[column], 0.75)
    stats['max'] = np.amax(data[column])
    stats['mean'] = np.mean(data[column])
    print('\nStats on column', column, ':')
    for k,v in stats.items():
        print(k+':\t'+str(v))
    return stats



#--------------------------------- MAIN ----------------------------------------

if __name__ == "__main__":
    #points = gpd.GeoDataFrame.from_file(realdata_path('bspoints.geojson'))
    #arclines = gpd.GeoDataFrame.from_file(realdata_path('arclines.geojson'))
    day09 = pd.read_csv(rawdata_path('sample1_cdr_2015-09-09.csv'), delimiter=';', nrows=200)
    day09.columns = ['UserID', 'Timestamp', 'LAC', 'CI']
    groups = day09.groupby(day09.iloc[:,0])
    print(groups.size())
    #for group in groups:
    #    print(group) # Class tuple
