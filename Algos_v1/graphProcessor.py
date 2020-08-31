import folium
import geopandas as gpd
import geopatra
import numpy as np
from shapely.geometry import Polygon, LineString, Point
from typing import List


def derive_centroids(trajectory: List['GwPoint']):
    centroids = gpd.GeoDataFrame(columns=['zone', 'geometry'])
    centroids.loc[0,'zone'], centroids.loc[0,'geometry'] = 'A', get_centroid(trajectory, 'A')
    centroids.loc[1,'zone'], centroids.loc[1,'geometry'] = 'B', get_centroid(trajectory, 'B')
    return centroids

def derive_distances(trajectory: List['GwPoint']):
    centroids = derive_centroids(trajectory)
    distances = gpd.GeoDataFrame(columns=['id', 'lac', 'distance', 'remote', 'geometry'])
    for idx, gw in enumerate(trajectory):
        distances.loc[idx,'id'], distances.loc[idx,'lac'] = gw.id, gw.lac
        distances.loc[idx,'geometry'] = Point(gw.longitude, gw.latitude)
        distances.loc[idx,'distance'] = get_distance_from_centroids(centroids, distances.loc[idx,'geometry'])
        distances.loc[idx,'remote'] = distances.loc[idx,'geometry'].distance(centroids.loc[0,'geometry']) # Distance wrt A"""
    return (centroids, distances)

def get_centroid(trajectory: List['GwPoint'], zone) -> Point:
    pts = []
    for t in trajectory:
        if t.zone == zone:
            pts.append([t.longitude, t.latitude])
    zdf = gpd.GeoDataFrame(columns=['geometry'])
    zdf.loc[0,'geometry'] = LineString(pts)
    cdf = gpd.GeoDataFrame(gpd.GeoSeries(zdf.centroid), columns=['geometry']) # geometry=point
    centroid = cdf.loc[0,'geometry']
    return centroid

def get_distance_from_centroids(centroids, point):
    distA = point.distance(centroids.loc[0,'geometry'])
    distB = point.distance(centroids.loc[1,'geometry'])
    distance = (distA + distB)
    return distance

def add_centroids_to_map(centroids, map):
    for idx, row in centroids.iterrows():
        point = row['geometry']
        folium.Marker(
            [point.y, point.x],
            tooltip='Centroid zone ' + row['zone'],
            icon=folium.Icon(color='lightgray', icon='circle', prefix="fa")
        ).add_to(map)

def get_min_distance(centroids):
    min_distance = get_distance_from_centroids(centroids, centroids.loc[0,'geometry']) # Raw distance from A to B
    return min_distance

def get_stats_on_rank(distances, centroids):
    stats = {}
    # Distance stats
    stats['min'] = get_min_distance(centroids)
    stats['q1'] = np.quantile(distances['distance'], 0.25)
    stats['med'] = np.median(distances['distance'])
    stats['q2'] = np.quantile(distances['distance'], 0.75)
    stats['max'] = np.amax(distances['distance'])
    # Remote stats
    stats['rmin'] = np.amin(distances['remote'])
    stats['rq1'] = np.quantile(distances['remote'], 0.25)
    stats['rmed'] = np.median(distances['remote'])
    stats['rq2'] = np.quantile(distances['remote'], 0.75)
    stats['rmax'] = np.amax(distances['remote'])
    return stats

def get_stats_on_agony(data):
    stats = {}
    stats['min'] = np.amin(data['agony'])
    stats['top'] = np.quantile(data['agony'], 0.1)
    stats['q1'] = np.quantile(data['agony'], 0.25)
    stats['med'] = np.median(data['agony'])
    stats['q2'] = np.quantile(data['agony'], 0.75)
    stats['max'] = np.amax(data['agony'])
    return stats

def compute_rank(centroids, distances, trajectory): # Final rank in [2;8]
    stats = get_stats_on_rank(distances, centroids)
    dist_rank, rem_rank = 0, 0
    for idx, row in distances.iterrows():
        if row['distance'] < stats['q1']:
            dist_rank = 4
        if row['distance'] >= stats['q1'] and row['distance'] < stats['med']:
            dist_rank = 3
        if row['distance'] >= stats['med'] and row['distance'] < stats['q2']:
            dist_rank = 2
        if row['distance'] >= stats['q2']:
            dist_rank = 1
        if row['remote'] < stats['rq1']:
            rem_rank = 4
        if row['remote'] >= stats['rq1'] and row['remote'] < stats['rmed']:
            rem_rank = 3
        if row['remote'] >= stats['rmed'] and row['remote'] < stats['rq2']:
            rem_rank = 2
        if row['remote'] >= stats['rq2']:
            rem_rank = 1
        distances.loc[idx, 'rank'] = dist_rank + rem_rank
        trajectory[idx].set_rank(distances.loc[idx, 'rank'])

def compute_agony(segments):
    for idx, row in segments.iterrows():
        if row['rank']:
            segments.loc[idx,'agony'] = 1/(row['weight']*row['rank'])
