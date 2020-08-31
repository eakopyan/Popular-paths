import folium
import geopatra
import geopandas as gpd
import numpy as np
import random
from datetime import datetime, timedelta
from typing import List
from shapely.geometry import Point
from scipy.spatial import Voronoi

from analyser import get_stats

realdata_path = '/home/agora/Documents/Popular_paths/Data/GeoJson/{}'.format
thresh = 120
radius_meter = 300
min_weight = 80
min_magnitude = 50


def create_map():
    map = folium.Map([45.73303, 4.82297], zoom_start=13)
    print('Created base station map.')
    return map


def add_voronoi(data, target_map):
    print('Adding Voronoi layer...')
    coords = []
    for idx, row in data.iterrows():
        coord = (row['geometry'].x, row['geometry'].y)
        if coord not in coords:
            coords.append(coord)
    input = np.array(coords)
    voronoi = Voronoi(input)
    (c_regions, c_colors) = assign_region_color(data, voronoi)
    fg = folium.FeatureGroup(name='Voronoi diagram', show=False)
    for reg_idx in voronoi.point_region:
        region = voronoi.regions[reg_idx]
        if -1 not in region and region != []:
            (cid, color) = get_region_color(c_regions, c_colors, reg_idx)
            print('Region', reg_idx, 'cluster', cid, 'color', color)
            region_coords = []
            for vertex in region:
                (x,y) = voronoi.vertices[vertex]
                region_coords.append((y,x))
            fg.add_child(folium.Polygon(
                region_coords,
                color = '#202020',
                weight = 2,
                fill = True,
                fill_color = color,
                fill_opacity = 0.3,
                tooltip = cid
            ))
    target_map.add_child(fg)

def get_random_color():
    rand_nb = random.randint(0, 16777215)
    hex_nb = str(hex(rand_nb))
    return '#'+hex_nb[2:]

def get_cluster_by_coord(data, coord):
    for idx, row in data.iterrows():
        (x,y) = (row['geometry'].x, row['geometry'].y)
        if (x,y) == (coord[0], coord[1]):
            if row['Zone'] == 'A':
                return '500'
            if row['Zone'] == 'B':
                return '700'
            return row['Cluster ID']
    return 'NaN'

def get_region_color(c_regions, c_colors, reg_idx):
    for cid, regions in c_regions.items():
        if reg_idx in regions:
            return (cid, c_colors[cid])

def assign_region_color(data, voronoi):
    cluster_reg, cluster_colors = {}, {}
    for idx, point in enumerate(voronoi.points):
        cluster = get_cluster_by_coord(data, point)
        if cluster == 'NaN':
            return 'No match found for point '+str(point)
        reg_idx = voronoi.point_region[idx]
        if cluster not in cluster_reg:
            cluster_reg[cluster] = [reg_idx]
        else:
            cluster_reg[cluster].append(reg_idx)
        cluster_colors[cluster] = get_random_color()
    return (cluster_reg, cluster_colors)

def add_places(data, map, name):
    print('Adding ClusterPoint layer...')
    fg = folium.FeatureGroup(name=name) # Name as it will appear in Layer control
    for idx, row in data.iterrows():
        """if row['Rank'] == 0:
            color = 'darkred'
        elif row['Rank'] < np.quantile(data['Rank'], 0.2): # Lowest ranks
            color = 'red'
        elif row['Rank'] >= np.quantile(data['Rank'], 0.2) and row['Rank'] < np.quantile(data['Rank'], 0.4):
            color = 'orange'
        elif row['Rank'] >= np.quantile(data['Rank'], 0.4) and row['Rank'] < np.quantile(data['Rank'], 0.6):
            color = 'green'
        elif row['Rank'] >= np.quantile(data['Rank'], 0.6) and row['Rank'] < np.quantile(data['Rank'], 0.8):
            color = 'blue'
        else:
            color = 'darkblue'"""
        fg.add_child(folium.CircleMarker(
            [row['geometry'].y, row['geometry'].x],
            radius = get_radius(row['Weight']),
            stroke = True,
            weight = 5,
            color = '#30AA00',
            fill = True,
            fill_color = '#66CD00',
            fill_opacity = 1,
            #tooltip = 'Rank: '+str(row['Rank']),
            tooltip = 'Cluster ID: '+str(row['Cluster ID'])
        ))
    map.add_child(fg)


def get_radius(weight):
    return int(weight/75)


def add_flows(data, map, name, emphasis=False, emph_color='#CC0000'):
    print('Adding ClusterFlow layer', name + '...')
    fg = folium.FeatureGroup(name=name)
    for idx, row in data.iterrows():
        color = '#60A100'
        if emphasis:
            if row['Path']:
                color = emph_color
                fg.add_child(folium.PolyLine(
                    row['geometry'].coords[:],
                    color = color,
                    weight = 15,
                    opacity = 0.9,
                    tooltip = 'Magnitude: '+str(row['Magnitude'])
                ))
        else:
            if row['Magnitude'] >= min_magnitude:
                fg.add_child(folium.PolyLine(
                    row['geometry'].coords[:],
                    color = color,
                    weight = get_weight(row['Magnitude']),
                    opacity = 0.8,
                    tooltip = 'Magnitude: '+str(row['Magnitude'])
                ))
    map.add_child(fg)


def get_weight(magnitude):
    return int(magnitude/50)


def create_legend():
    legend_html = """
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="utf-8">
            <style>
                #legend {
                    position: fixed;
                    background: #fff;
                    padding: 10px;
                    bottom: 20px;
                    left: 20px;
                    width: 250px;
                    border: 3px solid #000;
                    z-index: 1000;
                }
            </style>
        </head>
        <body>
            <div id="legend">
                <strong>Parameters</strong> <br>
                Threshold time: %s minutes <br>
                Cluster radius: %s meters <br>
                Minimum cluster weight: %s <br>
                Minimum displayed magnitude: %s
            </div>
        </body>
    </html>
    """ % (thresh, radius_meter, min_weight, min_magnitude)
    return legend_html


def add_legend(target_map):
    legend_html = create_legend()
    target_map.get_root().html.add_child(folium.Element(legend_html))


def close_map(map, filename):
    folium.LayerControl().add_to(map)
    add_legend(map)
    map.save(filename)
    print('Closing', filename, 'map.')


#------------------------------------------------------------------------------

if __name__ == "__main__":
    #map = create_map()
    SFOplaces = gpd.GeoDataFrame.from_file(realdata_path('SFO_300-80_clusterplaces.geojson'))
    SFOflows = gpd.GeoDataFrame.from_file(realdata_path('SFO_300-80_clusterflows.geojson'))

    #add_flows(SFOflows, map, 'Flows from Sainte-Foy')
    #add_places(SFOplaces, map, 'Places from Sainte-Foy')
    #add_voronoi(clusterpoints, map)

    #close_map(map, "Full week.html")
