import numpy as np
from typing import List, Dict
from tqdm import tqdm
from numpy.core._multiarray_umath import radians
from sklearn.cluster import DBSCAN

from dataProcessor import Place, Flow, ClusterPlace, ClusterFlow, get_people_count, get_flow_magnitude

def get_coordinates(places):
    return [[p.lon, p.lat] for p in places]

def to_ClusterPlace(clustered_places):
    clusters: List['ClusterPlace'] = []
    for id, places in clustered_places.items():
        if id != -1: # Not noise
            coordinates = get_coordinates(places)
            centroid = derive_centroid(coordinates)
            weight = len(places)
            clusters.append(ClusterPlace(
                str(id),
                centroid[0], # Longitude
                centroid[1], # Latitude
                weight
            ))
            for place in places:
                clusters[-1].place_ids.append(place.id)
                clusters[-1].rank.append(place.rank)
    return clusters

def get_clusterplace(set, id):
    for s in set:
        if s.id == str(id):
            return s

def to_ClusterFlow(clustered_flows, clusterplaces):
    clusters: List['ClusterFlow'] = []
    for id, flows in clustered_flows.items():
        magnitude = len(flows)
        cluster1 = get_clusterplace(clusterplaces, id[0])
        cluster2 = get_clusterplace(clusterplaces, id[1])
        clusters.append(ClusterFlow(
            str(id[0]), # (CI, LAC) first
            str(id[1]), # (CI, LAC) last
            (cluster1.lon, cluster1.lat),
            (cluster2.lon, cluster2.lat),
            magnitude
        ))
    return clusters

def derive_centroid(coordinates):
    array = np.array(coordinates)
    centroid = [np.mean(array[:,0]), np.mean(array[:,1])]
    return centroid

def get_cluster_assignments(radius_meter: float, min_measures: int, coordinates: List[List[float]]):
    km_radian = 6871.0088 # Conversion: kilometers per radian
    epsilon = radius_meter/1000/km_radian
    return DBSCAN(metric='haversine', algorithm='ball_tree', eps=epsilon, min_samples=min_measures).fit(
        radians(coordinates)).labels_

def assign_cluster(places, cluster_assignments: List[int]):
    clustered_places = {}
    with tqdm(total=len(places), desc='Place clustering') as pbar:
        for i, place in enumerate(places):
            cluster_id = cluster_assignments[i]
            place.cluster_id = cluster_id
            if cluster_id in clustered_places:
                clustered_places[cluster_id].append(place)
            else:
                clustered_places[cluster_id] = [place]
            pbar.update(1)
    return clustered_places

def assign_flow_cluster(flows: List[List['Flow']], cplaces: List['ClusterPlace']):
    clustered_flows: Dict[(int, int), List['Flow']] = {}
    count=0
    for user_flow in flows:
        count += len(user_flow)
    with tqdm(total=count, desc='Flow clustering') as pbar:
        for user_flow in flows:
            for flow in user_flow:
                cluster = (-1, -1)
                for cp in cplaces:
                    for pid in cp.place_ids:
                        if flow.id_first == pid:
                            cluster = (cp.id, cluster[1])
                        if flow.id_last == pid:
                            cluster = (cluster[0], cp.id)
                if cluster[0] != cluster[1] and -1 not in cluster: # Only consider external transitions
                    if cluster in clustered_flows:
                        clustered_flows[cluster].append(flow)
                    else:
                        clustered_flows[cluster] = [flow]
                pbar.update(1)
    return clustered_flows

def get_clusters(places: List['Place'], flows: List[List['Flow']], sfo_cluster, pdi_cluster, radius_meter: float, min_measures: int):
    coordinates: List[List[float]] = get_coordinates(places)
    cluster_ids: List[int] = get_cluster_assignments(radius_meter, min_measures, coordinates)
    clusters: Dict[int, List['Place']] = assign_cluster(places, cluster_ids)
    cluster_places = to_ClusterPlace(clusters)
    cluster_places.append(sfo_cluster)
    cluster_places.append(pdi_cluster)
    #fclusters: Dict[int, List['Flow']] = assign_flow_cluster(flows, cluster_places)
    #cluster_flows = to_ClusterFlow(fclusters, cluster_places)
    cluster_flows = []
    return [cluster_places, cluster_flows]
