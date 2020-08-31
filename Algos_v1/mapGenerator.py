import folium
import geopatra
import geopandas as gpd
from typing import List
from shapely.geometry import Point
from tqdm import tqdm

from graphProcessor import derive_centroids, derive_distances, compute_agony, get_stats_on_agony

realdata_path = '/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/maps/{}'.format
points = gpd.GeoDataFrame.from_file(realdata_path('points.geojson'))
segments = gpd.GeoDataFrame.from_file(realdata_path('segments.geojson'))


def create_map(data):
    """Creates main map centered on Lyon, and based on base station point array
    extracted from file "points.geojson".

    Points are clustered together by using geopatra's MarkerCluster (see
    geopatra.folium.markercluster for more information).

    Returns
    -------
    map : folium.map
        Map layer containing base station clusters
    """

    map = geopatra.folium.markercluster(
        data,
        name='Base stations',
        width='100%',
        height='100%',
        location=[45.73303, 4.82297],
        zoom=12
    )
    print('Created base station map.')
    return map


def add_agony_layer(seg, target_map):
    """Adds all segments as 5 distinct layers to the targetted map. Takes data
    from file "segments.geojson".

    Segments are sorted by weight (call to sort_segments_by_weight function).
    By default, category-1 segments are not displayed on the map.

    Parameters
    ----------
    target_map : folium.map
        Base map the layer will be added to.
    """

    stats = get_stats_on_agony(seg)
    fg = folium.FeatureGroup(name="Agony layer")
    for idx, row in seg.iterrows():
        fg.add_child(folium.PolyLine(
            row['geometry'].coords[:],
            tooltip = 'Agony: ' + str(row['agony']),
            opacity = 0.4,
            weight = 2, # Stroke width
            color = get_agony_color(stats, row['agony'])
        ))
    target_map.add_child(fg)


def get_agony_color(stats, agony):
    """Function to define a marker icon's color depending on given agony value.

    Parameters
    ----------
    stats : Dict[str: float]
        Statistics on dataset (min, q1, med, q2, max)
    agony : float
        Metric of the point to be colored

    Returns
    -------
    res : str
        Color string
    """

    if agony >= stats['q2']:
        return 'blue'
    if agony >= stats['med'] and agony < stats['q2']:
        return 'green'
    if agony >= stats['q1'] and agony < stats['med']:
        return 'yellow'
    if agony >= stats['top'] and agony < stats['q1']:
        return 'orange'
    if agony < stats['top']:
        return 'red'
    return 'lightgray'


def add_rank_layer(data, map):
    fg = folium.FeatureGroup(name='Base station ranks', show=False) # Name as it will appear in Layer control
    for idx, row in data.iterrows():
        fg.add_child(folium.Marker(
            [row['geometry'].y, row['geometry'].x],
            popup = 'ID: '+row['ID']+'\nLAC: '+row['LAC'],
            tooltip = 'Rank '+str(row['Rank']),
            icon = folium.Icon(color=get_rank_color(row['Rank']))
        ))
    map.add_child(fg)

def get_rank_color(rank):
    if rank < 4:
        return 'darkblue'
    if rank == 4:
        return 'blue'
    if rank == 5:
        return 'green'
    if rank == 6:
        return 'orange'
    if rank == 7:
        return 'red'
    if rank > 7:
        return 'darkred'
    return 'lightgray'

def close_map(map):
    """Function to save the created map in file "Lyon_map.html". Also adds a
    layer control (see folium.LayerControl for more information).

    Parameters
    ----------
    map : folium.map
        Map to be saved
    """

    folium.LayerControl().add_to(map)
    map.save("Lyon_map.html")
    print('Closing main map.')


#---------------------------------- MAIN --------------------------------------

if __name__ == "__main__":
    print('Creating base stations map...')
    main_map = create_map(points)
    #print(segments.head())

    # Computing agony
    compute_agony(segments)
    #print(segments.head())

    print('Creating agony-based layer...')
    add_agony_layer(segments, main_map)

    print('Creating rank-based layer...')
    #print(points.head())
    add_rank_layer(points, main_map)

    close_map(main_map)
