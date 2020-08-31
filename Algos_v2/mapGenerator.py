import folium
import geopatra
import geopandas as gpd
import numpy as np
from typing import List
from shapely.geometry import Point
from scipy.spatial import Voronoi

from analyser import get_stats

realdata_path = '/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/GeoJson/{}'.format
dataset_size = 2000
min_length = 3
min_weight = 4


def create_map():
    map = folium.Map([45.73303, 4.82297], zoom_start=13)
    print('Created base station map.')
    return map


def add_voronoi(data, target_map, name):
    print('Adding Voronoi layer...')
    coords = []
    for idx, row in data.iterrows():
        coord = (row['geometry'].x, row['geometry'].y)
        if coord not in coords:
            coords.append(coord)
    input = np.array(coords)
    voronoi = Voronoi(input)
    fg = folium.FeatureGroup(name=name, show=False)
    for ridge in voronoi.ridge_vertices:
        if -1 not in ridge: # Only draw finite regions
            (x1,x2) = voronoi.vertices[ridge[0]]
            (y1,y2) = voronoi.vertices[ridge[1]]
            fg.add_child(folium.PolyLine(
                [(x2,x1),(y2,y1)],
                opacity = 0.4,
                weight = 2, # Stroke width
                color = '#000050'
            ))
    target_map.add_child(fg)


def add_arclines(data, target_map):
    fg = folium.FeatureGroup(name="ArcLine layer")
    for idx, row in data.iterrows():
        if row['Weight'] >= min_weight:
            fg.add_child(folium.PolyLine(
                row['geometry'].coords[:],
                popup = 'Weight: '+ str(row['Weight']),
                opacity = 0.7,
                weight = row['Weight'], # Stroke width
                color = get_color(row['Agony'])
            ))
    target_map.add_child(fg)


def add_antpaths(data, target_map, name):
    print('Adding antpath layer...')
    fg = folium.FeatureGroup(name=name)
    stats = get_stats(data, 'Agony')
    for idx, row in data.iterrows():
        if row['Weight'] >= min_weight:
            fg.add_child(folium.plugins.antpath.AntPath(
                row['geometry'].coords[:],
                popup = 'Weight: '+str(row['Weight']),
                tooltip = 'Travel time: '+str(row['Travel time'])+' seconds',
                weight = row['Weight'], # Stroke width
                color = get_dist_color(stats, row['Agony']),
                dash_array = [10,50] # [Light pixels, dark pixels]
            ))
    target_map.add_child(fg)
    print('Done.')


def add_rank_layer(data, map):
    fg = folium.FeatureGroup(name='Base station ranks', show=False) # Name as it will appear in Layer control
    for idx, row in data.iterrows():
        fg.add_child(folium.CircleMarker(
            [row['geometry'].y, row['geometry'].x],
            radius = 10,
            color = get_rank_color(row['Rank']),
            stroke = False,
            fill = True,
            fill_opacity = 0.6,
            popup = 'ID: '+row['ID']+'\nLAC: '+row['LAC'],
            tooltip = 'Rank '+str(row['Rank']),
        ))
    map.add_child(fg)


def add_dist_layer(data, map):
    print('Adding distance layer...')
    fg = folium.FeatureGroup(name='Base station distances', show=False) # Name as it will appear in Layer control
    stats = get_stats(data, 'Distance')
    for idx, row in data.iterrows():
        fg.add_child(folium.CircleMarker(
            [row['geometry'].y, row['geometry'].x],
            radius = 10,
            color = get_dist_color(stats, row['Distance']),
            stroke = False,
            fill = True,
            fill_opacity = 0.6,
            popup = 'ID: '+row['ID']+'\nLAC: '+row['LAC'],
            tooltip = 'Distance: '+str(row['Distance']),
        ))
    map.add_child(fg)


def get_dist_color(stats, dist):
    if dist < stats['q1']:
        return '#66CD00'
    if dist >= stats['q1'] and dist < stats['med']:
        return 'yellow'
    if dist >= stats['med'] and dist < stats['q2']:
        return 'orange'
    if dist >= stats['q2']:
        return 'red'
    return 'lightgray'


def get_rank_color(rank):
    if rank == 0:
        return '#66CD00'
    if rank >= 1 and rank < 5:
        return 'yellow'
    if rank >= 5 and rank < 10:
        return 'orange'
    if rank >= 10 and rank < 15:
        return 'red'
    if rank >= 15:
        return 'black'
    return 'lightgray'


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
                Dataset: %s files <br>
                Minimum path length: %s steps <br>
                Minimum displayed arc weight: %s <br>
                Agony algorithm: zero value <br>
                Ranking: spatio-temporal
            </div>
        </body>
    </html>
    """ % (dataset_size, min_length, min_weight)
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
    mapAB, mapBA = create_map(), create_map()
    pointsAB = gpd.GeoDataFrame.from_file(realdata_path('pointsAB.geojson'))
    arclinesAB = gpd.GeoDataFrame.from_file(realdata_path('arclinesAB.geojson'))
    pointsBA = gpd.GeoDataFrame.from_file(realdata_path('pointsBA.geojson'))
    arclinesBA = gpd.GeoDataFrame.from_file(realdata_path('arclinesBA.geojson'))

    add_antpaths(arclinesAB, mapAB, name='Paths from A to B')
    add_voronoi(pointsAB, mapAB, name='Voronoi diagram')

    add_antpaths(arclinesBA, mapBA, name='Paths from B to A')
    add_voronoi(pointsBA, mapBA, name='Voronoi diagram')

    close_map(mapAB, "Ste-Foy.html")
    close_map(mapBA, "Pt-Dieu.html")
