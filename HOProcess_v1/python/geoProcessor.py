import folium
import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import List
from scipy.spatial import Voronoi
from shapely.geometry import Point, LineString, Polygon
from pyproj import Proj, transform, CRS

from dataProcessor import get_clusters, regroup

cells_path = "/home/agora/Documents/Handover/data/ECI_4G_WGS84.csv"
agg_ho_path="../data/HO_4G_AGG.csv"
nodes_path="../data/NODES.csv"
cluster_nodes_path="../data/clusternodes.geojson"
cluster_flows_path="../data/clusterflows.geojson"
points_path="../data/points.geojson"
flows_path="../data/flows.geojson"
survey_path="../data/SF2PD.geojson"

#===============================================================================
def add_voronoi(points, map):
    coords = [[p.x, p.y] for p in points['geometry']]
    colors = [[c] for c in points['Color']]
    clusters = [[cst] for cst in points['Cluster']]
    voronoi = Voronoi(np.array(coords))
    fg = folium.FeatureGroup(name='Voronoi diagram', show=False)
    for enb, reg_idx in enumerate(voronoi.point_region):
        region = voronoi.regions[reg_idx]
        if -1 not in region and region != []:
            color = colors[enb]
            region_coords = []
            for vertex in region:
                region_coords.append(voronoi.vertices[vertex])
            fg.add_child(folium.Polygon(
                region_coords,
                color = '#202020',
                weight = 2,
                fill = True,
                fill_color = color,
                fill_opacity = 0.3,
                tooltip = clusters[enb]
            ))
    map.add_child(fg)

def to_node(data):
    nodes: Dict[int, Node] = {}
    with tqdm(desc='Node conversion', total=len(data.index)) as pbar:
        for idx, row in data.iterrows():
            dest, src = int(row['EnB end']), int(row['EnB start'])
            if dest in nodes:
                nodes[dest][0] += int(row['Weight'])
            if src in nodes:
                nodes[src][1] += int(row['Weight'])
            if dest not in nodes:
                nodes[dest] = [int(row['Weight']), 0, float(row['x1']), float(row['y1'])] #Weight in, weight out, x, y
            if src not in nodes:
                nodes[src] = [0, int(row['Weight']), float(row['x2']), float(row['y2'])] #Weight in, weight out, x, y
            pbar.update(1)
    return nodes

def create_map():
    map = folium.Map([45.73303, 4.82297], zoom_start=13)
    print('Created base station map.')
    return map

def add_enb(data, map, name, cluster=True, rank='Georank', emphasis=True):
    print('Adding EnodeB layer...')
    fg = folium.FeatureGroup(name=name) # Name as it will appear in Layer control
    for idx, row in data.iterrows():
        id = row['Cluster']
        color = "#66CD00"
        if emphasis:
            if int(row[rank]) == 0:
                color = 'darkred'
            elif row[rank] < np.quantile(data[rank], 0.2): # Lowest ranks
                color = 'red'
            elif row[rank] >= np.quantile(data[rank], 0.2) and row[rank] < np.quantile(data[rank], 0.4):
                color = 'orange'
            elif row[rank] >= np.quantile(data[rank], 0.4) and row[rank] < np.quantile(data[rank], 0.6):
                color = 'green'
            elif row[rank] >= np.quantile(data[rank], 0.6) and row[rank] < np.quantile(data[rank], 0.8):
                color = 'blue'
            else:
                color = 'darkblue'
        fg.add_child(folium.CircleMarker(
            [row['geometry'].x, row['geometry'].y],
            radius = int(row['Weight in']/10000),
            #stroke = False,
            color = "#303030",
            fill = True,
            fill_color = color,
            fill_opacity = 0.8,
            tooltip = 'Cluster:'+str(id)+' '+rank+': '+str(row[rank])
        ))
    map.add_child(fg)

def add_places(data, map, name, rank):
    print('Adding Place layer...')
    fg = folium.FeatureGroup(name=name) # Name as it will appear in Layer control
    for idx, row in data.iterrows():
        id = row['Cluster ID']
        color = "#66CD00"
        fg.add_child(folium.CircleMarker(
            [row['geometry'].y, row['geometry'].x],
            radius = int(row['Weight']/75),
            #stroke = False,
            color = "#303030",
            fill = True,
            fill_color = color,
            fill_opacity = 0.8,
            tooltip = 'Cluster:'+str(id)
        ))
    map.add_child(fg)

def close_map(map, filename):
    folium.LayerControl().add_to(map)
    map.save('../maps/'+filename)
    print('Closing', filename, 'map.')

def get_flows(data):
    flows_gdf = gpd.GeoDataFrame(
        data.drop(['x1','y1','x2','y2'], axis=1),
        crs={'init':'epsg:4326'},
        geometry=[LineString([src, dest]) for src, dest in zip(zip(data.x2, data.y2), zip(data.x1, data.y1))]
    )
    return flows_gdf

def add_flows(data, map, name, min_weight=1000, color = '#AA00AA'):
    print('Adding Flow layer', name + '...')
    fg = folium.FeatureGroup(name=name)
    #max_weight = 0
    for idx, row in data.iterrows():
        if row['Weight'] >= min_weight:
            #if max_weight < row['Weight']:
                #max_weight = row['Weight']
            fg.add_child(folium.PolyLine(
                row['geometry'].coords[:],
                color = color,
                weight = int(row['Weight']*0.0005+0.526),
                opacity = 0.8,
                tooltip = 'Weight: '+str(row['Weight'])
            ))
    #print('Maximum weight in bounding box:', max_weight)
    map.add_child(fg)

def add_path(path, map, name, weight=12, color = '#AA0000', gt=False):
    print('Adding Path layer', name + '...')
    fg = folium.FeatureGroup(name=name)
    if not gt:
        total_weight = 0
        for idx, row in path.iterrows():
            total_weight += row['Weight']
            fg.add_child(folium.PolyLine(
                row['geometry'].coords[:],
                color = color,
                weight = weight,
                opacity = 0.9,
                tooltip = 'Weight: '+str(row['Weight'])
            ))
        print('Path length:', len(path.index))
        print('Path weight:', total_weight)
    else:
        for idx, row in path.iterrows():
            fg.add_child(folium.PolyLine(
                row['geometry'].coords[:],
                color = color,
                weight = 10,
                opacity = 0.9,
                tooltip = type
            ))
    map.add_child(fg)

def add_id_path(path, map, name, weight=12, color='#505050'):
    print('Adding Path layer', name + '...')
    fg = folium.FeatureGroup(name=name)
    total_weight = 0
    for idx, row in path.iterrows():
        total_weight += row['Magnitude']
        fg.add_child(folium.PolyLine(
            row['geometry'].coords[:],
            color = color,
            weight = weight,
            opacity = 0.9,
            tooltip = 'Weight: '+str(row['Magnitude'])
        ))
    print('Path length:', len(path.index))
    print('Path weight:', total_weight)
    map.add_child(fg)

def create_points(bbox):
    nodes_df = pd.read_csv(nodes_path, sep=';', names=['ID', 'Weight in', 'Weight out', 'Longitude', 'Latitude'])
    nodes_gdf = gpd.GeoDataFrame(
        nodes_df.drop(['Longitude', 'Latitude'], axis=1),
        crs={'init':'epsg:4326'},
        geometry=[Point(coord) for coord in zip(nodes_df.Longitude, nodes_df.Latitude)]
    )
    nodes_gdf = nodes_gdf.loc[nodes_gdf['geometry'].within(bbox)]
    print('Points in bounding box:', len(nodes_gdf.index))

    ho_df = pd.read_csv(agg_ho_path, sep=';')
    flows_gdf = get_flows(ho_df)
    flows_gdf = flows_gdf.loc[flows_gdf['geometry'].within(bbox)]
    print(nodes_gdf.head())
    print(flows_gdf.head())

    get_clusters(nodes_gdf, flows_gdf)
    print('Number of core (cluster) points:', len(nodes_gdf.loc[nodes_gdf['Cluster'] != -1]))
    #(nodes_gdf, flows_gdf) = regroup(nodes_gdf, flows_gdf)
    print('\nNew nodes and flows headers:')
    print(nodes_gdf.head())
    print(flows_gdf.head())
    nodes_gdf.to_file(cluster_nodes_path, driver='GeoJSON')
    flows_gdf.to_file(cluster_flows_path, driver='GeoJSON')


def create_flows(bbox):
    ho_df = pd.read_csv(agg_ho_path, sep=';')
    flows_gdf = get_flows(ho_df)
    flows_gdf = flows_gdf.loc[flows_gdf['geometry'].within(bbox)]
    print('\nFlows in bounding box:', len(flows_gdf.index))
    print(flows_gdf.head())

    """map = create_map()
    add_flows(flows_gdf, map, 'Flows', 1000)
    close_map(map, 'Handovers.html')"""

def create_survey_map(filename):
    map = folium.Map([45.73303, 4.82297], zoom_start=13)
    data = gpd.GeoDataFrame.from_file(survey_path)
    print(data.head())
    convert_crs(data)

    fg = folium.FeatureGroup(name="Survey")
    for idx, row in data.iterrows():
        type = row['Type']
        color = '#AA0000'
        if type == 'CAR':
            color = '#00AA00'
        if type == 'TCL':
            color = '#0000AA'
        fg.add_child(folium.PolyLine(
            row['geometry'].coords[:],
            color = color,
            weight = 5,
            opacity = 0.9,
            tooltip = type
        ))
    map.add_child(fg)
    close_map(map, filename)

def convert_crs(data):
    lambert = Proj('epsg:2154')
    wgs84 = Proj('epsg:4326')
    print('Number of paths:', len(data.index))
    for idx, row in data.iterrows():
        path = row['geometry'].coords[:]
        with tqdm(total=len(path), desc='Converting path '+str(idx)) as pbar:
            for i,p in enumerate(path):
                x,y = transform(lambert, wgs84, p[0], p[1])
                path[i] = [x,y]
                pbar.update(1)
            data.loc[idx,'geometry'].coords = path
    print(data)
    data.to_file('../data/GT_WGS84.geojson', driver = 'GeoJSON')

#===============================================================================
if __name__=='__main__':
    """
    print(ho_df.head())

    nodes = to_node(ho_df)
    nodes_df = pd.DataFrame.from_dict(nodes, orient='index', columns=['Weight in', 'Weight out', 'Longitude', 'Latitude'])
    nodes_df.to_csv(nodes_path, sep=';', index=True, header=False)"""

    x_min, y_min, x_max, y_max = 45.7011098, 4.7641685, 45.7870551, 4.8985538
    bbox = Polygon([(x_min, y_min), (x_min, y_max), (x_max, y_max), (x_max, y_min)])
    #create_points(bbox)
    #create_flows(bbox)

    """points = gpd.GeoDataFrame.from_file(points_path)
    flows = gpd.GeoDataFrame.from_file(flows_path)
    print(points.head())
    print(flows.head())

    map = create_map()
    add_flows(flows, map, 'Flows', 1000)
    add_enb(points, map, 'Clusters', cluster=True)
    close_map(map, 'Main map.html')"""

    """points = gpd.GeoDataFrame.from_file(cluster_nodes_path)
    flows = gpd.GeoDataFrame.from_file(cluster_flows_path)
    print(points.head())
    print(flows.head())

    (cluster_nodes, cluster_flows) = regroup(points, flows)
    print(cluster_nodes.head())
    print(cluster_flows.head())
    cluster_nodes.to_file(points_path, driver='GeoJSON')
    cluster_flows.to_file(flows_path, driver='GeoJSON')"""

    create_survey_map("Ground truth.html")
