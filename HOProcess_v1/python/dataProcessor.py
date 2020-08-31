import csv
import geopandas as gpd
import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta
from math import isnan
from numpy.core._multiarray_umath import radians
from pyproj import Proj, transform
from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import nearest_points
from sklearn.cluster import dbscan
from tqdm import tqdm
from typing import List, Dict

lambert = Proj('epsg:27572')
wgs84 = Proj('epsg:4326')


class Handover:
    def __init__(self, eci, tsp, enb_end, cell_end, enb_start, cell_start=None):
        self.eci = str(eci)
        self.timestamp = datetime.utcfromtimestamp(int(tsp)) # Type datetime
        self.enb_end = str(enb_end)
        self.cell_end = str(cell_end)
        self.enb_start = str(enb_start)
        self.cell_start = str(cell_start)

    def __str__(self):
        time = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return time+" Handover from ("+self.enb_start+";"+self.cell_start+") to ("+self.enb_end+";"+self.cell_end+") ECI "+self.eci


class EnodeB:
    def __init__(self, eci, lon, lat, enb_id=None, cell_id=None):
        self.eci = str(eci)
        self.enb_id = str(enb_id)
        self.cell_id = str(cell_id)
        self.lon = float(lon)
        self.lat = float(lat)

    def __str__(self):
        return "ECI: "+self.eci+"\tCoordinates: ("+str(self.lon)+";"+str(self.lat)+")"


#===============================================================================
def regroup(points, flows):
    cluster_nodes = points.drop(['ID'], axis=1).dissolve(by='Cluster', aggfunc='sum')
    cluster_nodes['geometry'] = cluster_nodes['geometry'].centroid
    cluster_nodes['Cluster'] = cluster_nodes.index

    cluster_flows = flows.drop(['ECI','EnB end', 'Cell end', 'EnB start'], axis=1).dissolve(by=['c_end', 'c_start'], aggfunc='sum')
    cluster_flows['c_end'], cluster_flows['c_start'] = -1, -1
    for (id1, id2), row in cluster_flows.iterrows():
        if id1 == id2: # Intra-cluster handovers, drop
            cluster_flows = cluster_flows.drop((id1,id2))
        cluster_flows.loc[(id1,id2),'c_end'], cluster_flows.loc[(id1,id2),'c_start'] = id1, id2
        src = cluster_nodes.loc[cluster_nodes['Cluster']==id1].geometry.values[0]
        dest = cluster_nodes.loc[cluster_nodes['Cluster']==id2].geometry.values[0]
        cluster_flows.loc[(id1,id2), 'geometry'] = LineString([src,dest])
    return (cluster_nodes, cluster_flows)

def cluster_flows(flows, points):
    unary_pts = points.geometry.unary_union
    flows['c_end'] = flows.apply(lambda x: find_nearest(x.geometry.coords[1], points, unary_pts), axis=1)
    flows['c_start'] = flows.apply(lambda x: find_nearest(x.geometry.coords[0], points, unary_pts), axis=1)

def find_nearest(coord, points, unary_pts):
    point = Point(coord)
    nearest = nearest_points(point, unary_pts)[1]
    res = points.loc[points['geometry']==nearest].Cluster.values[0]
    return res

def get_random_color():
    rand_nb = random.randint(0, 16777215)
    hex_nb = str(hex(rand_nb))
    return '#'+hex_nb[2:]

def get_clusters(points, flows, radius_meter=300, min_samples=80):
    coords = [[p.x, p.y] for p in points['geometry']]
    weights = points['Weight in']
    points['Cluster'] = get_cluster_assignments(radius_meter, min_samples, coords, weights)
    cluster_flows(flows, points)


def get_cluster_assignments(radius_meter: float, min_measures: int, coordinates: List[List[float]], weights):
    km_radian = 6871.0088 # Conversion: kilometers per radian
    epsilon = radius_meter/1000/km_radian
    #return DBSCAN(metric='haversine', algorithm='ball_tree', eps=epsilon, min_samples=min_measures).fit(
    #    radians(coordinates)).labels_
    return dbscan(radians(coordinates), eps=epsilon, min_samples=min_measures, metric='haversine', algorithm='ball_tree',
        sample_weight=weights)[1] # returns: tuple(core_samples, labels)

def group_flows(data):
    agg_dict = {} # Key = ECI, value = [{['ECI', 'Weight', 'EnB end', 'Cell end', 'EnB start', 'Cell start']}]
    with tqdm(desc='Aggregation', total=len(data.index)) as pbar:
        for idx, row in data.iterrows():
            if not isnan(row['EnB start']):
                key = (int(row['ECI']), int(row['EnB start']))
                if key not in agg_dict:
                    agg_dict[key] = {
                        'ECI':row['ECI'],
                        'Weight':1,
                        'EnB end':row['EnB end'],
                        'Cell end':row['Cell end'],
                        'EnB start':row['EnB start'],
                        'Cell start':row['Cell start']
                    }
                else:
                    values = agg_dict[key]
                    values['Weight'] += 1
            pbar.update(1)
    return agg_dict


def fill_coordinates(data, nodes):
    data['x1'], data['y1'] = 0, 0
    for idx, row in data.iterrows():
        if idx >= 3:
            break
        with tqdm(desc='Filling coordinates', total=len(nodes)) as pbar:
            for node in nodes:
                if int(row['ECI']) == int(node.eci):
                    data.loc[idx,'x1'] = node.lon
                    data.loc[idx,'y1'] = node.lat
                pbar.update(1)
    print(data.head())

def find_eci(enb_id, data):
    cells = []
    visited = []
    print('Match(es) for EnB ID', enb_id)
    for idx, row in data.iterrows():
        cell = int(row['Cell end'])
        if cell not in cells:
            cells.append(cell)
        if int(row['EnB end']) == enb_id:
            if cell not in visited:
                visited.append(cell)
                print(cell)
    print(sorted(cells))

def check_doubles(data):
    hist = {}
    with tqdm(desc='Checking EnB ID', total=len(data.index)) as pbar:
        for idx, row in data.iterrows():
            enb = row['EnB end']
            cell = row['Cell end']
            if enb in hist:
                if cell != hist[enb]: # Same ECI for different EnB
                    print('Different cell found for EnB', enb)
                    print('Existing:', hist[enb])
                    print('Found:', cell)
                    break
            else:
                hist[enb] = cell
            pbar.update(1)

def parse_handovers(data):
    handovers: List['Handover'] = []
    with tqdm(desc='Parsing handovers', total=len(data.index)) as pbar:
        for idx, row in data.iterrows():
            if idx >= 5:
                break
            handovers.append(Handover(
                eci=int(row['ECI']),
                tsp=row['Timestamp'],
                enb_end=int(row['EnB end']),
                cell_end=int(row['Cell end']),
                enb_start=int(row['EnB start']),
                cell_start=row['Cell start']
            ))
            pbar.update(1)
    return handovers

def parse_enodeb(filename:str):
    nodes: List['EnodeB'] = []
    with open(filename, "r") as f:
        csv_reader = csv.reader(f, delimiter = ';')
        with tqdm(desc='Parsing EnodeB') as pbar:
            next(csv_reader, None)
            for line in csv_reader:
                if len(line) > 0:
                    node = EnodeB(
                        eci=line[0],
                        lon=line[1],
                        lat=line[2]
                    )
                    #to_crs(node)
                    nodes.append(node)
                pbar.update(1)
    return nodes

def export_coordinates(nodes: List['EnodeB'], path:str):
    eci, lon, lat = [], [], []
    for node in nodes:
        eci.append(node.eci)
        lon.append(node.lon)
        lat.append(node.lat)
    df = pd.DataFrame(data={'ECI':eci, 'Longitude':lon, 'Latitude':lat})
    df.to_csv(path, sep=';', index=False)
    print('Coordinates export complete.')

def to_crs(node: 'EnodeB'):
    x,y = node.lon, node.lat
    node.lon, node.lat = transform(lambert, wgs84, x, y)
