import folium
import numpy as np
import pandas as pd
import geopandas as gpd

data_path = '/home/agora/Documents/Popular_paths/Data/GeoJson/{}'.format

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
    cluster_places = gpd.GeoDataFrame.from_file(data_path('full_clusterplaces.geojson'))
    cluster_flows = gpd.GeoDataFrame.from_file(data_path('full_clusterflows.geojson'))

    get_stats(cluster_places, 'Weight')
    get_stats(cluster_flows, 'Magnitude')
