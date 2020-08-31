import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import List, Dict
from optparse import OptionParser

from dataProcessor import ClusterPlace, ClusterFlow, Step, Timestep, preprocess_data, get_supergraph, get_people_count, get_flow_magnitude
from spatialSimp import get_clusters, get_coordinates, derive_centroid
from geoProcessor import ClusterPoint, ClusterLine, PlacePoint, create_geojson_clusters, create_geojson_lines, create_geojson_points

user_dir_path = "/home/agora/Documents/Popular_paths/Data/data/traces/"
gjson_dir_path = "/home/agora/Documents/Popular_paths/Data/GeoJson/"
analytics_path = "/home/agora/Documents/Popular_paths/Data/Analytics/"
cells_path = "/home/agora/Documents/Popular_paths/Data/data/final_cells.csv"
steps_path="/home/agora/Documents/Popular_paths/Data/data/full_steps.csv"
ranks_path="/home/agora/Documents/Popular_paths/Data/data/"


"""Notations:
    SFO: Sainte-Foy
    PDI: Part-Dieu
    'SFO-PDI' files: from Sainte-Foy to Part-Dieu
"""

def filter_clusters(cplaces, cflows):
    for cp in cplaces:
        present = False
        for cf in cflows:
            if cp.id in (cf.id_first, cf.id_last):
                present = True
        if not present:
            cplaces.remove(cp)
    return cplaces

def create_raw_graph(places, flows):
    cplaces, cflows = {}, {}
    for p in places:
        if p.id not in cplaces:
            cplaces[p.id] = [p]
        else:
            cplaces[p.id].append(p)
    for user_flow in flows:
        for flow in user_flow:
            if (flow.id_first, flow.id_last) in cflows:
                cflows[(flow.id_first, flow.id_last)].append(flow)
            else:
                cflows[(flow.id_first, flow.id_last)] = [flow]
    cluster_places: List['ClusterPlace'] = []
    cluster_flows: List['ClusterFlow'] = []
    index = 0
    for id, place in cplaces.items():
        cluster_places.append(ClusterPlace(index, place[0].lon, place[0].lat, len(place)))
        for p in place:
            cluster_places[-1].rank.append(p.rank)
        cluster_places[-1].place_ids = [place[0].id]
        index += 1
    for id, flow in cflows.items():
        cluster_flows.append(ClusterFlow(
            get_id(cluster_places, flow[0].id_first),
            get_id(cluster_places, flow[0].id_last),
            flow[0].coord_first,
            flow[0].coord_last,
            len(flow)
        ))
    return (cluster_places, cluster_flows)

def get_id(cluster_places, id):
    for cp in cluster_places:
        if cp.place_ids[0] == id:
            return cp.id

def process_fromSFO(paths: Dict[str, List[List['Step']]], radius_meter, min_measures, thresh):
    print('\n======================== from SFO to PDI ==========================')
    print('Parsing places and flows...')
    [places, flows] = get_supergraph(paths)
    [pcount, ranks] = get_people_count(places)
    flow_magnitude = get_flow_magnitude(flows)
    print('Number of places:', len(pcount), '\tNumber of flows:', len(flow_magnitude))

    (sfo_cluster, pdi_cluster) = get_endpoint_clusters(places)
    print('Clusters for Sainte-Foy and Part-Dieu:')
    print(sfo_cluster)
    print(pdi_cluster)
    trans_places: List['Place'] = []
    for p in places:
        if p.zone == 'C':
            trans_places.append(p)
    cluster_places: List['ClusterPlace'] = []
    cluster_flows: List['ClusterFlow'] = []
    print('Creating clusters with radius', radius_meter, 'm and at least', min_measures, 'points...')
    [cplaces, cflows] = get_clusters(trans_places, flows, sfo_cluster, pdi_cluster, radius_meter, min_measures)
    print('Number of ClusterPlace:', len(cplaces), '\tNumber of ClusterFlow:', len(cflows))

    place_points = create_geojson_points(places)
    print('Exporting data to', gjson_dir_path+'SFO'+'_'+str(thresh)+'_placepoints.geojson...')
    PlacePoint.export(place_points, gjson_dir_path+'SFO'+'_'+str(thresh)+"_placepoints.geojson")

    """if len(cflows) > 0:
        cluster_places = filter_clusters(cplaces, cflows)
        cluster_flows = cflows
    #(cluster_places, cluster_flows) = create_raw_graph(places, flows)
    print('Post-filtering number of ClusterPlace:', len(cluster_places), '\tNumber of ClusterFlow:', len(cluster_flows))
    export_ranks(cluster_places, 'SFO_'+str(thresh))
    export_gjson(cluster_places, cluster_flows, 'SFO', thresh)"""


def get_endpoint_clusters(places: List['Place']):
    sfo_places: List['Place'] = []
    pdi_places: List['Place'] = []
    for p in places:
        if p.zone == 'A':
            sfo_places.append(p)
        elif p.zone == 'B':
            pdi_places.append(p)
    sfo_centroid = derive_centroid(get_coordinates(sfo_places))
    sfo_cluster = ClusterPlace(str(500), sfo_centroid[0], sfo_centroid[1], len(sfo_places))
    for place in sfo_places:
        sfo_cluster.place_ids.append(place.id)
        sfo_cluster.rank.append(place.rank)
    pdi_centroid = derive_centroid(get_coordinates(pdi_places))
    pdi_cluster = ClusterPlace(str(700), pdi_centroid[0], pdi_centroid[1], len(pdi_places))
    for place in pdi_places:
        pdi_cluster.place_ids.append(place.id)
        pdi_cluster.rank.append(place.rank)
    return (sfo_cluster, pdi_cluster)


def process_fromPDI(paths: Dict[str, List[List['Step']]], radius_meter, min_measures):
    print('\n======================== from PDI to SFO ==========================')
    print('Parsing places and flows...')
    [places, flows] = get_supergraph(paths)
    [pcount, ranks] = get_people_count(places)
    flow_magnitude = get_flow_magnitude(flows)
    print('Number of places:', len(pcount), '\tNumber of flows:', len(flow_magnitude))
    cluster_places: List['ClusterPlace'] = []
    cluster_flows: List['ClusterFlow'] = []
    print('Creating clusters with radius', radius_meter, 'm and at least', min_measures, 'points...')
    [cplaces, cflows] = get_clusters(places, flows, radius_meter, min_measures)
    print('Number of ClusterPlace:', len(cplaces), '\tNumber of ClusterFlow:', len(cflows))
    if len(cflows) > 0:
        cluster_places = filter_clusters(cplaces, cflows)
        cluster_flows = cflows
    print('Post-filtering number of ClusterPlace:', len(cluster_places), '\tNumber of ClusterFlow:', len(cluster_flows))
    export_ranks(cluster_places, 'PDI')
    export_gjson(cluster_places, cluster_flows, 'PDI')


def export_gjson(cluster_places, cluster_flows, src: str, thresh):
    clusters_gjson = create_geojson_clusters(cluster_places)
    print('Exporting data to', gjson_dir_path+src+'_'+str(thresh)+'_clusterplaces.geojson...')
    ClusterPoint.export(clusters_gjson, gjson_dir_path+src+'_'+str(thresh)+"_clusterplaces.geojson")
    flows_gjson = create_geojson_lines(cluster_flows)
    print('Exporting data to', gjson_dir_path+src+'_'+str(thresh)+'_clusterflows.geojson...')
    ClusterLine.export(flows_gjson, gjson_dir_path+src+'_'+str(thresh)+"_clusterflows.geojson")

def compute_avg_length(paths: Dict[str, List[List['Step']]]):
    length, count = 0.0, 0
    for uid, path_set in paths.items():
        for path in path_set:
            length += len(path)
        count += len(path_set)
    length = length/count
    print('Number of paths:', count, '\tAverage path length:', length)

def export_travel_time(paths: Dict[str, List[List['Step']]], filename):
    travel_times = {}
    for path_set in paths.values():
        for path in path_set:
            time = Timestep(path[0].timestamp, path[-1].timestamp)
            if len(path) not in travel_times:
                travel_times[len(path)] = [time.delta.seconds]
            else:
                travel_times[len(path)].append(time.delta.seconds)
    avg_times = {}
    for length, times in sorted(travel_times.items()):
        avg_times[length] = np.mean(times)
        #print('Length:', length, '\tAverage travel time:', avg_times[length])
    df = pd.DataFrame(data={'Path Length': list(avg_times.keys()), 'Travel time (s)': list(avg_times.values())})
    df.to_csv(filename, sep=';', index=False)

def export_steps(steps: List['Step']):
    uid, tsp, ci, lac, lon, lat, zone = [], [], [], [], [], [], []
    for step in steps:
        uid.append(step.user_id)
        tsp.append(step.timestamp)
        ci.append(int(step.bs_id[0]))
        lac.append(int(step.bs_id[1]))
        lon.append(step.lon)
        lat.append(step.lat)
        zone.append(step.zone)
    df = pd.DataFrame(data={'User ID':uid, 'Timestamp':tsp, 'CI':ci, 'LAC':lac, 'Longitude':lon, 'Latitude':lat, 'Zone':zone})
    df.to_csv(steps_path, sep=';', index=False)

def export_ranks(cplaces: List['ClusterPlace'], src: str):
    id, ranks = [], []
    for cp in cplaces:
        id.append(cp.id)
        rank = ""
        for r in cp.rank:
            rank += str(r) + "-"
        ranks.append(rank)
    df = pd.DataFrame(data={'Cluster ID':id, 'Ranks':ranks})
    df.to_csv(ranks_path+'ranks_'+src+'.csv', sep=';', index=False)
    print('Exported ranks in file', ranks_path+'ranks_'+src+'.csv')

def parse_steps():
    steps: List['Step'] = []
    with open(steps_path, 'r') as f:
        with tqdm(desc='Parsing steps') as pbar:
            for line in f.readlines():
                elem = line.split(";")
                if len(elem[0]) > 10:
                    steps.append(Step(
                        elem[0], # User ID
                        elem[1], # Timestamp
                        (elem[2], elem[3]), # (CI, LAC)
                        elem[4], # Longitude
                        elem[5], # Latitude
                        elem[6] # Zone
                    ))
                pbar.update(1)
    return steps

def split_directions(paths:Dict[str, List[List['Step']]]):
    paths_fromSFO, paths_fromPDI = {}, {}
    for uid, path_set in paths.items():
        for path in path_set:
            if path[0].zone == 'A': # Source SFO
                if uid in paths_fromSFO:
                    paths_fromSFO[uid].append(path)
                else:
                    paths_fromSFO[uid] = [path]
            elif path[0].zone == 'B': # Source PDI
                if uid in paths_fromPDI:
                    paths_fromPDI[uid].append(path)
                else:
                    paths_fromPDI[uid] = [path]
    return (paths_fromSFO, paths_fromPDI)

#------------------------------------------------------------------------------

if __name__ == "__main__":
    steps: List['Step'] = parse_steps()
    print('Number of raw complete steps:', len(steps))

    #thresholds = [30,45,60,75,90,105,120]
    #for thresh in thresholds:
    thresh = 120
    paths: Dict[str, List[List['Step']]] = preprocess_data(steps, thresh)
    (paths_fromSFO, paths_fromPDI) = split_directions(paths)
    len_SFO, len_PDI = 0,0
    for path_set in paths_fromSFO.values():
        len_SFO += len(path_set)
    for path_set in paths_fromPDI.values():
        len_PDI += len(path_set)
    print('Paths from Sainte-Foy:', len_SFO, '\tPaths from Part-Dieu:', len_PDI)

    radius, samples = 300, 80
    process_fromSFO(paths_fromSFO, radius, samples, thresh)
    #process_fromPDI(paths_fromPDI, radius, samples)
