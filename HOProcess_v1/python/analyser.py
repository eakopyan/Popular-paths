import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.spatial.distance import directed_hausdorff
from shapely.geometry import Point, LineString, Polygon
from tqdm import tqdm
from typing import List, Dict

from geoProcessor import get_flows
#from network import drop_noise

flows_path="../data/flows.geojson"
medianpoints_path="../data/points_medianrank.geojson"
agopoints_path="../data/points_agorank.geojson"
geopoints_path="../data/points_georank.geojson"
gt_path="../data/GT_WGS84.geojson"
mp_path="../data/path_Medianrank.geojson"

prop_flows_path="/home/agora/Documents/Popular_paths/Data/GeoJson/SFO_120_clusterflows.geojson"
prop_places_path="/home/agora/Documents/Popular_paths/Data/GeoJson/SFO_120_clusterplaces.geojson"

agg_ho_path="../data/HO_4G_AGG.csv"
nodes_path="../data/NODES.csv"


#===============================================================================
def print_features(nodes, arcs):
    print('Graph order (number of nodes):', len(nodes.index))
    print('Graph degree (number of arcs):', len(arcs.index))
    matrix = get_adjacence_matrix(nodes, arcs, dataset=1)
    max_co = len(matrix)*len(matrix)
    print('Number of possible connections:', max_co)
    adjacence = len(arcs.index)/max_co*100
    """if connectivity < 50:
        print('Weakly connected graph')
    else:
        print('Strongly connected graph')"""
    print('Adjacence ratio:', adjacence)
    pm = get_path_matrix(nodes, arcs, matrix, dataset=1)

def get_path_matrix(nodes, arcs, matrix, dataset=1):
    pm = matrix
    if dataset == 1:
        for i in range(len(pm)):
            for j in range(len(pm)):
                src, dest = i,j
                if src != dest:
                    res = find_path(pm, src, dest)
                    if res == 1:
                        pm[src][dest] = res
    count = 0
    for line in pm:
        for i in line:
            count += i
    print('Connectivity:', count)
    return pm

def find_path(matrix, src, dest):
    if matrix[src][dest] == 1: # Adjacent vertices
        return 1
    else:
        neighbors: List[int] = []
        for v in matrix[src]:
            if v == 1:
                neighbors.append(v)
        for n in neighbors:
            find_path(matrix, n, dest)

def get_adjacence_matrix(nodes, arcs, dataset=1): # 1 for HO, 2 for propagation
    matrix = np.zeros((len(nodes.index),len(nodes.index)), dtype=int)
    if dataset == 2:
        for idx, row in arcs.iterrows():
            i,j = int(row['Source ID']), int(row['Destination ID'])
            if i == 500: # SFO cluster
                i = len(matrix)-2
            if j == 700: # PDI cluster
                j = len(matrix)-1
            matrix[i][j] = 1
    else:
        for idx, row in arcs.iterrows():
            i,j = row['c_start'], row['c_end']
            matrix[i][j] = 1
    return matrix

def get_noiseless_features(points, flows):
    min_weight = np.quantile(flows['Weight'], 0.75)
    print('Initial dataset weights:')
    df = flows['Weight']
    print(df.describe())
    print('Number of non isolated nodes:', len(points.index))

    noise = flows.loc[flows['Weight'] < min_weight].index
    flows.drop(noise, inplace=True)
    nodes = points.loc[points['Cluster'].isin(flows['c_end'])]
    print('\nFinal dataset weights:')
    df = flows['Weight']
    print(df.describe())
    print('Number of non isolated nodes:', len(nodes.index))

def get_stats_on_bbox():
    x_min, y_min, x_max, y_max = 45.7011098, 4.7641685, 45.7870551, 4.8985538
    bbox = Polygon([(x_min, y_min), (x_min, y_max), (x_max, y_max), (x_max, y_min)])
    nodes_df = pd.read_csv(nodes_path, sep=';', names=['ID', 'Weight in', 'Weight out', 'Longitude', 'Latitude'])
    nodes_gdf = gpd.GeoDataFrame(
        nodes_df.drop(['Longitude', 'Latitude'], axis=1),
        crs={'init':'epsg:4326'},
        geometry=[Point(coord) for coord in zip(nodes_df.Longitude, nodes_df.Latitude)]
    )
    print('NaN-filtered nodes')
    df = nodes_gdf['Weight in']
    print(df.describe())
    nodes_gdf = nodes_gdf.loc[nodes_gdf['geometry'].within(bbox)]
    print('\nBbox nodes')
    df = nodes_gdf['Weight in']
    print(df.describe())
    points = gpd.GeoDataFrame.from_file(geopoints_path)
    flows = gpd.GeoDataFrame.from_file(flows_path)
    print('\nClustered nodes')
    df = points['Weight in']
    print(df.describe())
    min_weight = np.quantile(flows['Weight'], 0.75) # Around 400, mean around 6000
    flows.drop(flows.loc[flows['Weight'] < min_weight].index, inplace=True)
    nodes = points.loc[(points['Cluster'].isin(flows['c_end'])) | (points['Cluster'].isin(flows['c_start']))]
    print('\nMaxcost nodes')
    df = points['Weight in']
    print(df.describe())

    """ho_df = pd.read_csv(agg_ho_path, sep=';')
    flows_gdf = get_flows(ho_df)
    print('Number of raw flows:', len(flows_gdf.index))
    df = flows_gdf['Weight']
    print(df.describe())
    flows_gdf = flows_gdf.loc[flows_gdf['geometry'].within(bbox)]
    print('\nNumber of flows in bbox:', len(flows_gdf.index))
    df = flows_gdf['Weight']
    print(df.describe())"""

def extract_coords(data):
    coords = []
    for idx, row in data.iterrows():
        coords.extend(row['geometry'].coords[:])
    return np.array(coords)

def compare_paths(path):
    gt = gpd.GeoDataFrame.from_file(gt_path)
    path_coords = extract_coords(path)
    distances = []
    for i in range(1,11):
        gt_coords = extract_coords(gt.loc[gt['ID']==str(i)])
        distance = directed_hausdorff(gt_coords, path_coords)[0]
        #print('Distance with respect to path', str(i)+':', distance)
        distances.append(distance)
    print('Shortest distance %.6f' % min(distances),'with path(s)', [i+1 for i,e in enumerate(distances) if e==min(distances)])
    #return [str(i+1) for i,e in enumerate(distances) if e==min(distances)]
    return(min(distances), [str(i+1) for i,e in enumerate(distances) if e==min(distances)][0])

def get_traffic_proportion(path, flows, ranking):
    #flows = gpd.GeoDataFrame.from_file(flows_path)
    #drop_noise(flows, points, ranking)
    traffic, tp = sum(flows['Weight']), sum(path['Weight'])
    print('Traffic proportion for', ranking+':', tp/traffic*100)

def get_id_traffic_proportion(path, flows, ranking):
    traffic, tp = sum(flows['Magnitude']), sum(path['Magnitude'])
    print('Traffic proportion for', ranking+':', tp/traffic*100)

#===============================================================================
if __name__=="__main__":
    gt = gpd.GeoDataFrame.from_file(gt_path)
    print(gt)
    #compare_paths(maxpath)
