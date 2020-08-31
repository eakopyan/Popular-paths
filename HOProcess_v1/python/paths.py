import folium
import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import List, Dict
from shapely.geometry import Point, LineString, Polygon

from geoProcessor import *
from ranking import get_flow_neighbors

flows_path="../data/flows.geojson"
medianpoints_path="../data/points_medianrank.geojson"
agopoints_path="../data/points_agorank.geojson"
geopoints_path="../data/points_georank.geojson"
gt_path="../data/GT_WGS84.geojson"


class Node: # For BFS processing
    def __init__(self, id, rank, cost, path):
        self.id = id # Cluster ID
        self.rank = float(rank) # Rank of point
        self.cost = int(cost) # Cost of path so far (magnitude)
        self.path: List[int] = path # Current path so far, list of cluster IDs

    def __str__(self):
        return 'Place '+str(self.id)+', rank '+str(self.rank)+' from path of length '+str(len(self.path))+'\tCost: '+str(self.cost)

#===============================================================================

def compute_maxcost(points, flows, rankfunc, src_id=145, dest_id=6):
    dest_rank = points.loc[points['Cluster']==dest_id, rankfunc].values[0]
    maxpath: List[int] = [src_id] # Final path to return, list of cluster IDs
    queue: List['Node'] = [Node(src_id, 0, 0, maxpath)] # List of nodes to process
    max_weight = 0 # Weight of maxpath
    min_weight = np.quantile(flows['Weight'], 0.75) # Around 400, mean around 6000
    noise = flows.loc[flows['Weight'] < min_weight].index
    flows.drop(noise, inplace=True)
    while queue:
        current_node = queue.pop() # Remove and return last node
        if current_node.cost > max_weight: # Found a stronger path
            max_weight = current_node.cost
            maxpath = current_node.path
            print('Weight:', max_weight, maxpath)
        if current_node.id != dest_id: # Destination not reached
            neighbors = get_flow_neighbors(current_node.id, flows, points)
            nodes = points.loc[(points['Cluster'].isin(neighbors['c_end'])) & (points[rankfunc] > current_node.rank)]
            nodes.drop(nodes.loc[nodes[rankfunc] > dest_rank].index, inplace=True)
            for idx, row in nodes.iterrows():
                cid = int(row['Cluster'])
                if cid not in current_node.path: # Don't allow cycles
                    new_path = current_node.path + [cid]
                    weight = neighbors.loc[(neighbors['c_start']==current_node.id) & (neighbors['c_end']==cid), 'Weight'].values[0]
                    queue.append(Node(cid, row[rankfunc], current_node.cost + int(weight), new_path))
    return maxpath

def compute_median(flows):
    points = gpd.GeoDataFrame.from_file(medianpoints_path)
    print(points.head())
    maxpath = compute_maxcost(points, flows, 'Medianrank')
    print('Maximum cost path by median ranking:')
    print(maxpath)

def compute_ago(flows):
    points = gpd.GeoDataFrame.from_file(agopoints_path)
    print(points.head())
    maxpath = compute_maxcost(points, flows, 'Agorank')
    print('Maximum cost path by agony ranking:')
    print(maxpath)

def compute_geo(flows):
    points = gpd.GeoDataFrame.from_file(geopoints_path)
    print(points.head())
    maxpath = compute_maxcost(points, flows, 'Georank')
    print('Maximum cost path by geographic ranking:')
    print(maxpath)

def export_path(maxpath, flows, points, ranking):
    flows['Path'] = False
    for i, n in enumerate(maxpath):
        if i+1 < len(maxpath):
            c_start, c_end = n, maxpath[i+1]
            flows.loc[(flows['c_start']==c_start) & (flows['c_end']==c_end), 'Path'] = True
    path = flows.loc[flows['Path']==True]
    print(path)
    map = create_map()
    add_flows(flows, map, 'Flows', 1000)
    add_path(path, map, 'Maxpath')
    add_enb(points, map, 'Clusters', cluster=True, rank=ranking)
    close_map(map, 'maxpath.html')
    path.to_file('../data/path_'+ranking+'.geojson', driver = 'GeoJSON')
    print('Exported path to file', 'path_'+ranking+'.geojson')

def create_comparative_map(paths, gt_ids, filename):
    map = folium.Map([45.73303, 4.82297], zoom_start=13)
    data = gpd.GeoDataFrame.from_file(gt_path)
    fg = folium.FeatureGroup(name="Ground truth")
    gt = data.loc[data['ID'].isin(gt_ids)]
    for idx, row in gt.iterrows():
        fg.add_child(folium.PolyLine(
            row['geometry'].coords[:],
            color = '#00AA00',
            weight = 5,
            opacity = 0.8,
            tooltip = row['Type']
        ))
    map.add_child(fg)
    for path in paths:
        add_path(path, map, 'Closest path', weight=5)
    close_map(map, filename+'.html')

#===============================================================================
if __name__=='__main__':
    flows = gpd.GeoDataFrame.from_file(flows_path)
    points = gpd.GeoDataFrame.from_file(geopoints_path)
    print(flows.head(), '\n')

    #compute_ago(flows)
    #compute_geo(flows)
    #compute_median(flows)
    #maxpath = [145, 141, 120, 155, 5, 102, 166, 18, 23, 8, 62, 15, 113, 68, 66, 121, 0, 33, 32, 125, 14, 90, 85, 106, 65, 10, 54, 29, 6]
    #maxpath = [145, 141, 120, 155, 5, 102, 27, 148, 103, 16, 108, 23, 8, 62, 15, 113, 68, 66, 121, 0, 33, 32, 125, 14, 90, 85, 106, 65, 10, 54, 29, 6]
    #maxpath = [145, 141, 120, 155, 5, 4, 102, 166, 18, 23, 8, 62, 15, 113, 68, 66, 121, 0, 33, 32, 125, 14, 90, 85, 106, 65, 10, 54, 29, 6]
    #maxpath = [145, 141, 120, 155, 5, 4, 102, 27, 148, 103, 16, 108, 23, 8, 62, 15, 113, 68, 66, 121, 0, 33, 32, 125, 14, 90, 85, 106, 65, 10, 54, 29, 6]
    #maxpath = [145, 141, 120, 155, 5, 4, 13, 60, 23, 8, 62, 15, 113, 68, 66, 121, 0, 33, 32, 125, 14, 90, 85, 106, 65, 10, 54, 29, 6]
    export_path(maxpath, flows, points, 'Georank')
