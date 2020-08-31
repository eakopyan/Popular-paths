import folium
import geopandas as gpd
import pandas as pd
import numpy as np
import time
from tqdm import tqdm
from typing import List, Dict
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import nearest_points

from geoProcessor import *

points_path="../data/points.geojson"
flows_path="../data/flows.geojson"
points_v3_path = '/home/agora/Documents/Popular_paths/Data/GeoJson/SFO_120_clusterplaces.geojson'
ranks_v3_path = '/home/agora/Documents/Popular_paths/Data/data/ranks_SFO_120.csv'
medianpoints_path="../data/points_medianrank.geojson"
agopoints_path="../data/points_agorank.geojson"
geopoints_path="../data/points_georank.geojson"


def assign_georank(points, src_id=145, precision=10000):
    print('\nAssigning ranks by geographic distance...')
    src = points.loc[points['Cluster']==src_id]
    ref = Point(src['geometry'].x, src['geometry'].y)
    points['Georank'] = precision*points.distance(ref)

def get_flow_neighbors(id, flows, points): # Only allow outcoming flows
    neighbors = flows.loc[(flows['c_start'] == id) & (flows['c_end'] != id)]
    return neighbors

def format_endpoints(flows, src_id, dest_id): # Format to have SFO as source and PDI as destination
    print('Number of flows:', len(flows.index))
    noise = flows.loc[(flows['c_end'] == src_id) | (flows['c_start'] == dest_id)]
    oriented = flows.drop(noise, inplace=True, axis=0)
    print('New number of flows:', len(oriented.index), '\tPrevious:', len(flows.index))
    return oriented

def assign_medianrank(points, flows, src_id=145, dest_id=6): # Legacy code
    points_range: Dict[int, List[int]] = {} # List of possible ranges for each point
    print('\nAssigning ranks by median value...')
    points['Medianrank'] = -1
    queue = [(-1, src_id)] # Source and destination, -1 for initialization
    visited = []
    count = 0
    while queue:
        if count == 1000:
            print('Queue size:', len(queue))
            #time.sleep(3)
            count = 0
        (src, node_id) = queue.pop()
        if (src, node_id) not in visited:
            #print('Checking on point', node_id, '(from source '+str(src)+')')
            visited.append((src, node_id))
            neighbors = get_flow_neighbors(node_id, flows, points)
            nodes = points.loc[points['Cluster'].isin(neighbors['c_end'])]
            for idx, row in nodes.iterrows():
                if (node_id, row['Cluster']) not in queue:
                    queue.append((node_id, row['Cluster']))
            if src == -1: # Init
                points_range[node_id] = [0]
            else:
                if node_id not in points_range:
                    points_range[node_id] = [r+1 for r in points_range[src]]
                else:
                    points_range[node_id].extend([r+1 for r in points_range[src]])
        count += 1
    for idx, row in points.iterrows():
        points.loc[idx, 'Medianrank'] = np.median(points_range[row['Cluster']])
        print('Assigned cluster', row['Cluster'], 'with rank', row['Medianrank'])

def assign_agorank(points, flows, src_id=145, scale=100):
    print('\nAssigning ranks by agony optimization...')
    points['Agorank'] = -1
    points.loc[points['Cluster']==src_id, 'Agorank'] = 0
    queue = [src_id]
    visited = []
    while queue:
        node_id = queue.pop()
        if node_id not in visited:
            visited.append(node_id)
            neighbors = get_flow_neighbors(node_id, flows, points)
            nodes = points.loc[points['Cluster'].isin(neighbors['c_end'])]
            for idx, row in nodes.iterrows():
                if row['Cluster'] not in queue:
                    queue.append(row['Cluster'])
            for id, row in neighbors.iterrows():
                min_agony = scale
                rank_src = points.loc[points['Cluster']==node_id, 'Agorank'].values[0]
                rank_dest = nodes.loc[nodes['Cluster']==row['c_end'], 'Agorank'].values[0]
                if (rank_src, rank_dest) == (-1,-1): # Undefined
                    for rs in range(1, scale+1): # Could be less, could be more. Arbitrary.
                        for rd in range(1, scale+1):
                            agony = int(row['Weight'] * (rs - rd + 1))
                            if max(agony,0) < min_agony:
                                min_agony = max(agony,0)
                                rank_src = rs
                                rank_dest = rd
                elif rank_src == -1:
                    for rs in range(1, scale+1):
                        agony = int(row['Weight'] * (rs - rank_dest + 1))
                        if max(agony,0) < min_agony:
                            min_agony = max(agony,0)
                            rank_src = rs
                elif rank_dest == -1:
                    for rd in range(1, scale+1):
                        agony = int(row['Weight'] * (rank_src - rd + 1))
                        if max(agony,0) < min_agony:
                            min_agony = max(agony,0)
                            rank_dest = rd
                points.loc[points['Cluster']==node_id, 'Agorank'] = rank_src
                points.loc[points['Cluster']==row['c_end'], 'Agorank'] = rank_dest
            print('Assigned cluster', node_id, 'with rank', rank_src)

def remove_noise(points, flows):
    noise = points.loc[points['Cluster'] == -1].index
    points.drop(noise, inplace=True)
    noise = flows.loc[(flows['c_end'] == -1) | (flows['c_start'] == -1)].index
    flows.drop(noise, inplace=True)
    print('\nNoise reduction:')
    print(points.head())
    print(flows.head())
    points.to_file(points_path, driver='GeoJSON')
    flows.to_file(flows_path, driver='GeoJSON')

def format_ranks(data):
    ranks: Dict[int, List[int]] = {}
    for id, row in data.iterrows():
        raw = [r for r in row['Ranks'].split('-')]
        values = []
        for r in raw:
            if r != '':
                values.append(int(r))
        if str(row['Cluster ID']) not in ranks:
            ranks[str(row['Cluster ID'])] = sorted(values)
        else:
            for v in sorted(values):
                ranks[str(row['Cluster ID'])].append(v)
    return ranks

def find_nearest(coord, df, unary_pts):
    point = Point(coord)
    nearest = nearest_points(point, unary_pts)[1]
    return df.loc[df['geometry']==nearest].Rank.values[0]

def get_v3_ranks(points):
    places_df = gpd.GeoDataFrame.from_file(points_v3_path)
    ranks_df = pd.read_csv(ranks_v3_path, sep=';')
    ranks: Dict[int, List[int]] = format_ranks(ranks_df)
    places_df['Rank'] = -1
    for id, r in ranks.items():
        places_df.loc[places_df['Cluster ID'] == id, 'Rank'] = np.median(r)
    unary_pts = places_df.geometry.unary_union
    points['Medianrank'] = points.apply(lambda row: find_nearest((row.geometry.y, row.geometry.x), places_df, unary_pts), axis=1)

def display_ranking():
    medianpoints = gpd.GeoDataFrame.from_file(medianpoints_path)
    agopoints = gpd.GeoDataFrame.from_file(agopoints_path)
    geopoints = gpd.GeoDataFrame.from_file(geopoints_path)
    print(medianpoints.head())
    print(agopoints.head())
    print(geopoints.head())
    map = create_map()
    add_enb(medianpoints, map, 'Median value', cluster=True, rank='Medianrank')
    add_enb(agopoints, map, 'Agony optimization', cluster=True, rank='Agorank')
    add_enb(geopoints, map, 'Geographic distance', cluster=True, rank='Georank')
    close_map(map, 'Ranking.html')

#==============================================================================
if __name__=='__main__':
    display_ranking()

    """points = gpd.GeoDataFrame.from_file(points_path)
    flows = gpd.GeoDataFrame.from_file(flows_path)
    print(points.head())
    print(flows.head())

    # Source: SFO cluster in [104,132,141,145,134]
    #assign_georank(points)
    #assign_agorank(points, flows)
    #get_v3_ranks(points)

    print(points.head())
    points.to_file('points_medianrank.geojson', driver='GeoJSON')

    map = create_map()
    add_flows(flows, map, 'Flows', 1000)
    add_enb(points, map, 'Clusters', cluster=True, rank='Medianrank')
    close_map(map, 'Main map.html')"""
