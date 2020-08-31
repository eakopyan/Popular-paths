import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import List, Dict

from dataProcessor import ClusterPlace, ClusterFlow, parse_file, preprocess_data, parse_basestations, get_supergraph, get_people_count, get_flow_magnitude
from spatialSimp import get_clusters
from geoProcessor import ClusterPoint, ClusterLine, create_geojson_clusters, create_geojson_lines
from mapGenerator import *
from popularity import export_maxpath
from parser import parse_steps, preprocess_data, export_travel_time

user_dir_path = "/home/agora/Documents/Popular_paths/Data/data/traces/"
geojson_path = '/home/agora/Documents/Popular_paths/Data/GeoJson/{}'.format
analytics_path = "/home/agora/Documents/Popular_paths/Data/Analytics/"
cells_path = "/home/agora/Documents/Popular_paths/Data/data/final_cells.csv"

def compute_avg_length(paths: Dict[str, List[List['Step']]]):
    total, count = 0.0, 0
    for uid, path_set in paths.items():
        for path in path_set:
            total += len(path)
        count += len(path_set)
    length = total/count
    print('Average path length:', length)

def filter_clusters(cplaces, cflows):
    for cp in cplaces:
        present = False
        for cf in cflows:
            if cp.id in (cf.id_first, cf.id_last):
                present = True
        if not present:
            cplaces.remove(cp)
    return cplaces

def create_raw(steps, dataset):
    paths: Dict[str, List[List['Step']]] = preprocess_data(steps, dataset, 60, filter=False)
    compute_avg_length(paths)
    print('Parsing places and flows...')
    [places, flows] = get_supergraph(paths, dataset)
    print('Number of users:', len(flows))
    people_count = get_people_count(places)
    flow_magnitude = get_flow_magnitude(flows)
    print('Number of places:', len(people_count), '\tNumber of flows:', len(flow_magnitude))
    cluster_places: List['ClusterPlace'] = []
    for p in places:
        cluster_places.append(ClusterPlace(
            42,
            p.lon,
            p.lat,
            weight=1
        ))
    cluster_flows: List['ClusterFlow'] = []
    for user_flow in flows:
        for f in user_flow:
            cluster_flows.append(ClusterFlow(
                42,
                53,
                f.coord_first,
                f.coord_last,
                mag=1
            ))
    clusters_gjson = create_geojson_clusters(cluster_places)
    print('Exporting data to', gjson_dir_path+options.date+'_rawplaces.geojson...')
    ClusterPoint.export(clusters_gjson, gjson_dir_path + options.date + "_rawplaces.geojson")
    flows_gjson = create_geojson_lines(cluster_flows)
    print('Exporting data to', gjson_dir_path+options.date+'_rawflows.geojson...')
    ClusterLine.export(flows_gjson, gjson_dir_path + options.date + "_rawflows.geojson")

def create_filtered(steps, dataset):
    paths: Dict[str, List[List['Step']]] = preprocess_data(steps, dataset, 60)
    compute_avg_length(paths)
    print('Parsing places and flows...')
    [places, flows] = get_supergraph(paths, dataset)
    print('Number of users:', len(flows))
    people_count = get_people_count(places)
    flow_magnitude = get_flow_magnitude(flows)
    print('Number of places:', len(people_count), '\tNumber of flows:', len(flow_magnitude))
    cluster_places: List['ClusterPlace'] = []
    for p in places:
        cluster_places.append(ClusterPlace(
            42,
            p.lon,
            p.lat,
            weight=1
        ))
    cluster_flows: List['ClusterFlow'] = []
    for user_flow in flows:
        for f in user_flow:
            cluster_flows.append(ClusterFlow(
                42,
                53,
                f.coord_first,
                f.coord_last,
                mag=1
            ))
    clusters_gjson = create_geojson_clusters(cluster_places)
    print('Exporting data to', gjson_dir_path+options.date+'_filteredplaces.geojson...')
    ClusterPoint.export(clusters_gjson, gjson_dir_path + options.date + "_filteredplaces.geojson")
    flows_gjson = create_geojson_lines(cluster_flows)
    print('Exporting data to', gjson_dir_path+options.date+'_filteredflows.geojson...')
    ClusterLine.export(flows_gjson, gjson_dir_path + options.date + "_filteredflows.geojson")

def create_aggregated(steps, dataset):
    paths: Dict[str, List[List['Step']]] = preprocess_data(steps, dataset, 60)
    compute_avg_length(paths)
    print('Parsing places and flows...')
    [places, flows] = get_supergraph(paths, dataset)
    print('Number of users:', len(flows))
    people_count = get_people_count(places)
    flow_magnitude = get_flow_magnitude(flows)
    print('Number of places:', len(people_count), '\tNumber of flows:', len(flow_magnitude))
    cluster_places: List['ClusterPlace'] = []
    cluster_flows: List['ClusterFlow'] = []
    radius_meter, min_measures = 300, 15
    print('Creating clusters with radius', radius_meter, 'm and at least', min_measures, 'points...')
    (cplaces, cflows) = get_clusters(places, flows, radius_meter, min_measures)
    if len(cflows) > 0:
        cluster_places = filter_clusters(cplaces, cflows)
        cluster_flows = cflows
    print('Post-filtering number of ClusterPlace:', len(cluster_places), '\tNumber of ClusterFlow:', len(cluster_flows), '\n')
    clusters_gjson = create_geojson_clusters(cluster_places)
    print('Exporting data to', gjson_dir_path+options.date+'_aggplaces.geojson...')
    ClusterPoint.export(clusters_gjson, gjson_dir_path + options.date + "_aggplaces.geojson")
    flows_gjson = create_geojson_lines(cluster_flows)
    print('Exporting data to', gjson_dir_path+options.date+'_aggflows.geojson...')
    ClusterLine.export(flows_gjson, gjson_dir_path + options.date + "_aggflows.geojson")

def export_attributes(paths: Dict[str, List[List['Step']]], thresh):
    lengths: Dict[int, int] = {}
    for uid, path_set in paths.items():
        for path in path_set:
            if len(path) in lengths:
                lengths[len(path)] += 1
            else:
                lengths[len(path)] = 1
    df = pd.DataFrame.from_dict(lengths, orient='index', columns=['Count'])
    print(df.head())
    df.to_csv(analytics_path+'path_lengths_'+str(thresh)+'.csv', sep=';', index=True)
    print('Exported path lengths in file', analytics_path+str(thresh)+'_path_lengths.csv')

#------------------------------------------------------------------------------

if __name__ == "__main__":
    """
    ppaths_georank_sfo = [
        [500, 90, 14, 15, 16, 5, 6, 21, 32, 46, 19, 700],
        [500, 2, 0, 14, 42, 5, 6, 21, 32, 46, 19, 700],
        [500, 2, 62, 14, 42, 5, 6, 21, 32, 46, 19, 700],
        [500, 2, 14, 42, 5, 6, 21, 17, 32, 46, 19, 700],
        [500, 2, 14, 15, 16, 5, 6, 21, 32, 46, 19, 700],
        [500, 9, 15, 16, 42, 5, 6, 21, 32, 46, 19, 700],
        [500, 20, 14, 15, 16, 5, 6, 21, 32, 46, 19, 700],
        [500, 14, 15, 16, 5, 6, 21, 17, 32, 46, 19, 700],
        [500, 14, 15, 16, 42, 5, 6, 21, 32, 46, 19, 700],
        [500, 8, 9, 15, 16, 5, 6, 21, 32, 46, 19, 700]
    ]
    weights_georank_sfo = [3677, 3710, 3722, 3738, 3899, 3948, 4105, 4118, 4180, 4204]
    """

    flows_df = gpd.GeoDataFrame.from_file(geojson_path('SFO_300-80_clusterflows.geojson'))
    total_weight = np.sum(flows_df['Magnitude'])
    print('Total weight:', total_weight)
