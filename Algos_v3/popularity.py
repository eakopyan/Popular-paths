import pandas as pd
import geopandas as gpd
import numpy as np
from typing import List, Dict
from scipy.spatial import distance

from mapGenerator import *

geojson_path = '/home/agora/Documents/Popular_paths/Data/GeoJson/{}'.format
ranks_path = '/home/agora/Documents/Popular_paths/Data/data/{}'.format

class Place:
    def __init__(self, cluster_id, weight, lon, lat):
        self.cluster_id = int(cluster_id)
        self.weight = int(weight)
        self.lon = float(lon)
        self.lat = float(lat)
        self.rank = -1
        self.dist = 0.0

    def __str__(self):
        return ' ID '+str(self.cluster_id)+' Weight '+str(self.weight)+' Rank '+str(self.rank)


class Flow:
    def __init__(self, src_id, dest_id, magnitude):
        self.src_id = int(src_id)
        self.dest_id = int(dest_id)
        self.magnitude = int(magnitude)

    def __str__(self):
        return 'ID ('+str(self.src_id)+','+str(self.dest_id)+') Magnitude '+str(self.magnitude)


class Node: # For BFS processing
    def __init__(self, id, rank, cost, path):
        self.id = id # ID of place (cluster)
        self.rank = rank # Rank of place
        self.cost = cost # Cost of path so far (magnitude)
        self.path: List['Place'] = path # Current path so far

    def __str__(self):
        return 'Place '+str(self.id)+', rank '+str(self.rank)+' from path of length '+str(len(self.path))+'\tCost: '+str(self.cost)


def get_ranks(data):
    ranks: Dict[int, List[int]] = {}
    for id, row in data.iterrows():
        raw = [r for r in row['Ranks'].split('-')]
        values = []
        for r in raw:
            if r != '':
                values.append(int(r))
        if int(row['Cluster ID']) not in ranks:
            ranks[int(row['Cluster ID'])] = sorted(values)
        else:
            for v in sorted(values):
                ranks[int(row['Cluster ID'])].append(v)
    return ranks

def compute_distances(places, precision=100):
    src = places[500]
    for id, p in places.items():
        places[id].dist = precision*distance.euclidean([p.lon, p.lat], [src.lon, src.lat])

def compute_agony(flows, ranks, places):
    compute_distances(places)
    for id, flow in flows.items():
        min_agony = 1000000
        rank_src, rank_dest = -1, -1
        src, dest = places[flow.src_id], places[flow.dest_id]
        for rs in ranks[flow.src_id]:
            for rd in ranks[flow.dest_id]:
                #agony = flow.magnitude * (rs*src.dist - rd*dest.dist)
                agony = flow.magnitude * (rs - rd + 1)
                if max(agony,0) < min_agony:
                    min_agony = max(agony,0)
                    rank_src = rs
                    rank_dest = rd
        ranks[flow.src_id] = [rank_src]
        ranks[flow.dest_id] = [rank_dest]
    return ranks

def assign_ranks(ranks, places, df):
    df['Rank'] = 0
    for id in places.keys():
        places[id].rank = ranks[id][0]
        for i, row in df.iterrows():
            if int(row['Cluster ID']) == id:
                df.loc[i,'Rank'] = places[id].rank

def assign_georanks(places, df, precision=100):
    compute_distances(places, precision)
    df['Rank'] = 0
    for id, p in places.items():
        places[id].rank = int(places[id].dist)
        for i, row in df.iterrows():
            if int(row['Cluster ID']) == id:
                df.loc[i,'Rank'] = places[id].rank

def assign_medianrank(ranks, places, df):
    df['Rank'] = 0
    for id, r in ranks.items():
        places[id].rank = np.median(r)*10 # Keep precision
        for i, row in df.iterrows():
            if int(row['Cluster ID']) == id:
                df.loc[i,'Rank'] = places[id].rank

def get_endpoints(places, src_id, dest_id):
    return (places[src_id], places[dest_id])

def get_flow_neighbors(id, places, flows): # Only allow outcoming flows
    neighbors: Dict[int, List['Place']] = {}
    connections: Dict[tuple, List['Flow']] = {}
    flows_list: List['Flow'] = []
    for fid, flow in flows.items():
        if flow.magnitude >= 5:
            if flow.src_id == id:
                n = places[flow.dest_id]
                neighbors[n.cluster_id] = n
                flows_list.append(flow)
    for flow in sorted(flows_list, key=lambda k:k.magnitude, reverse=True):
        connections[(flow.src_id, flow.dest_id)] = flow
    return (neighbors, connections)

def compute_maxcost(places, flows, max_length, min_weight=500):
    src = places[500] # Cluster ID of Sainte-Foy
    dest = places[700] # Cluster ID of Part-Dieu
    maxpath: List['Place'] = [src] # Final path to return
    queue: List['Node'] = [Node(src.cluster_id, src.rank, 0, maxpath)] # List of neighboring nodes to process
    max_weight = 0 # Weight of maxpath
    while queue:
        current_node = queue.pop() # Remove and return last node
        if max_weight < current_node.cost: # Found a stronger path
            if len(current_node.path) <= max_length:
                max_weight = current_node.cost
                maxpath = current_node.path
                print('Weight', max_weight, [p.cluster_id for p in maxpath])
                #if max_weight > min_weight and current_node.id == dest.cluster_id:
                #    break
        if current_node.id != dest.cluster_id:
            (neighbors, connections) = get_flow_neighbors(current_node.id, places, flows)
            for id, n in neighbors.items():
                if n.rank >= current_node.rank and n not in current_node.path: # Don't allow cycles
                    n_path = current_node.path + [n]
                    flow = connections[(current_node.id, n.cluster_id)]
                    queue.append(Node(
                        n.cluster_id,
                        n.rank,
                        current_node.cost + flow.magnitude,
                        n_path
                    ))
    return maxpath

def export_maxpath(maxpath, flows_df):
    flows_df['Path'] = False
    for idx, place in enumerate(maxpath):
        if idx+1 < len(maxpath):
            flow_id = (place.cluster_id, maxpath[idx+1].cluster_id)
            for i, row in flows_df.iterrows():
                if (int(row['Source ID']), int(row['Destination ID'])) == flow_id:
                    flows_df.loc[i,'Path'] = True
                    #print('Found match for flow', flow_id)

def create_graph():
    places = [
        Place(0,1),
        Place(1,1),
        Place(2,1),
        Place(3,1),
        Place(4,1)
    ]
    places[0].rank = 0
    places[1].rank = 1
    places[2].rank = 1
    places[3].rank = 2
    places[4].rank = 3

    flows = [
        Flow(0,1,1),
        Flow(0,2,2),
        Flow(1,3,2),
        Flow(2,4,1),
        Flow(3,4,1)
    ]
    return (places, flows)

def get_place(places, id):
    for p in places:
        if p.cluster_id == id:
            return p

def to_places(data):
    places: Dict[int, Place] = {}
    for id, row in data.iterrows():
        places[int(row['Cluster ID'])] = Place(
            row['Cluster ID'],
            row['Weight'],
            row['geometry'].x,
            row['geometry'].y
        )
    return places

def to_flows(data):
    flows: Dict[tuple, Flow] = {}
    for id, row in data.iterrows():
        flows[(int(row['Source ID']),int(row['Destination ID']))] = Flow(
            row['Source ID'],
            row['Destination ID'],
            row['Magnitude']
        )
    return flows

def process_ranks(ranks):
    for id, rl in ranks.items():
        thresh_low = np.quantile(rl, 0.25) # Lowest values
        thresh_high = np.quantile(rl, 0.9) # Highest values
        new_ranks = []
        for r in rl:
            if r >= thresh_low and r <= thresh_high:
                if r not in new_ranks:
                    new_ranks.append(r)
        ranks[id] = new_ranks
    """key_places = [500,700,6,16,19,21,32]
    key_places = [6,500,700]
    for id,r in ranks.items():
        if id in key_places:
            print('\nCounts for place', id)
            hist = {}
            for elem in r:
                if elem not in hist:
                    hist[elem] = 1
                else:
                    hist[elem] += 1
            for k in hist.items():
                print(k)"""


def process_SFO():
    # 120-minute threshold data
    #places_df = gpd.GeoDataFrame.from_file(geojson_path('SFO_120_clusterplaces.geojson'))
    #flows_df = gpd.GeoDataFrame.from_file(geojson_path('SFO_120_clusterflows.geojson'))
    #ranks_df = pd.read_csv(ranks_path('ranks_SFO_120.csv'), sep=';')
    #points_df = gpd.GeoDataFrame.from_file(geojson_path('SFO_120_placepoints.geojson'))

    # 30-minute threshold data
    places_df = gpd.GeoDataFrame.from_file(geojson_path('SFO_30_clusterplaces.geojson'))
    flows_df = gpd.GeoDataFrame.from_file(geojson_path('SFO_30_clusterflows.geojson'))
    ranks_df = pd.read_csv(ranks_path('ranks_SFO_30.csv'), sep=';')

    places: Dict[int, Place] = to_places(places_df)
    flows: Dict[tuple, Flow] = to_flows(flows_df)
    ranks: Dict[int, List[int]] = get_ranks(ranks_df)

    # Assigning ranks
    """print('Computing agony...')
    process_ranks(ranks)
    final_ranks = compute_agony(flows, ranks, places)
    print('Assigning final ranks...')
    assign_ranks(final_ranks, places, places_df)"""

    #assign_georanks(places, places_df, precision=10000)
    assign_medianrank(ranks, places, places_df)

    #maxpath = compute_maxcost(places, flows, max_length=9, min_weight=3000)

    # Threshold 120 minutes
    """ppaths_georank_sfo = [
        [500, 9, 15, 16, 42, 5, 6, 21, 32, 46, 19, 700],
        [500, 20, 14, 15, 16, 5, 6, 21, 32, 46, 19, 700],
        [500, 14, 15, 16, 5, 6, 21, 17, 32, 46, 19, 700],
        [500, 14, 15, 16, 42, 5, 6, 21, 32, 46, 19, 700],
        [500, 8, 9, 15, 16, 5, 6, 21, 32, 46, 19, 700]
    ]
    ppaths_agony_sfo = [
        [500, 20, 14, 21, 15, 16, 17, 5, 6, 46, 19, 700],
        [500, 20, 14, 21, 42, 15, 16, 5, 6, 46, 19, 700],
        [500, 14, 21, 15, 16, 17, 32, 5, 6, 46, 19, 700],
        [500, 14, 21, 42, 15, 16, 32, 5, 6, 46, 19, 700],
        [500, 14, 21, 42, 15, 16, 17, 5, 6, 46, 19, 700]
    ]
    ppaths_median_sfo = [
        [500, 22, 16, 15, 21, 5, 6, 17, 32, 46, 19, 700],
        [500, 20, 22, 16, 15, 21, 5, 6, 32, 46, 19, 700],
        [500, 20, 14, 16, 15, 21, 5, 6, 32, 46, 19, 700],
        [500, 14, 42, 16, 15, 21, 5, 6, 32, 46, 19, 700],
        [500, 14, 22, 16, 15, 21, 5, 6, 32, 46, 19, 700]
    ]"""

    # Threshold 30 minutes
    """ppaths_georank_sfo = [
        [500, 14, 18, 9, 4, 5, 15, 44, 11, 10, 12, 700],
        [500, 14, 18, 9, 4, 5, 15, 11, 10, 12, 700],
        [500, 14, 18, 9, 4, 5, 15, 17, 11, 10, 12, 700],
        [500, 13, 14, 18, 9, 4, 5, 15, 11, 10, 12, 700],
        [500, 7, 8, 18, 9, 4, 5, 15, 11, 10, 12, 700]
    ]
    ppaths_agony_sfo = [
        [500, 14, 15, 4, 5, 18, 21, 6, 10, 12, 700],
        [500, 14, 15, 4, 5, 18, 21, 6, 10, 11, 12, 700],
        [500, 14, 15, 4, 5, 18, 20, 21, 6, 10, 12, 700],
        [500, 13, 14, 15, 4, 5, 17, 6, 10, 11, 12, 700],
        [500, 13, 14, 15, 4, 5, 17, 51, 6, 10, 12, 700]
    ]
    ppaths_median_sfo = [
        [500, 14, 9, 18, 15, 4, 5, 17, 11, 10, 12, 700],
        [500, 19, 9, 18, 15, 4, 5, 17, 11, 10, 12, 700],
        [500, 13, 19, 9, 18, 15, 4, 5, 11, 10, 12, 700],
        [500, 13, 14, 9, 18, 15, 4, 5, 11, 10, 12, 700],
        [500, 7, 8, 9, 18, 15, 4, 5, 11, 10, 12, 700]
    ]"""

    ppaths_median_sfo = [
    [500, 8, 15, 4, 5, 11, 10, 12, 700],
    [500, 15, 4, 5, 11, 10, 12, 700],
    [500, 15, 4, 5, 17, 11, 10, 12, 700],
    [500, 14, 30, 15, 4, 5, 11, 12, 700],
    [500, 14, 15, 4, 5, 11, 10, 12, 700]
    ]

    colors = ['#FFD700', '#FFB90F', '#FFA010', '#FF6000', '#BB0000']

    print('Creating map...')
    map = create_map()
    add_flows(flows_df, map, 'Flows from Sainte-Foy')
    add_places(places_df, map, 'Places from Sainte-Foy')
    #add_voronoi(points_df, map)
    #export_maxpath(maxpath, flows_df)
    #add_flows(flows_df, map, 'Popular path', emphasis=True)

    for pos, maxpath in enumerate(ppaths_median_sfo):
        for i, id in enumerate(maxpath):
            maxpath[i] = places[id]
        export_maxpath(maxpath, flows_df)
        add_flows(flows_df, map, 'Popular path'+str((5-pos)), emphasis=True, emph_color=colors[pos])
    close_map(map, "Popular paths T30.html")

def export_gjson_path(ids, ranking):
    flows = gpd.GeoDataFrame.from_file(geojson_path('SFO_30_clusterflows.geojson'))
    flows['Path'] = False
    for i, n in enumerate(ids):
        if i+1 < len(ids):
            c_start, c_end = str(n), str(ids[i+1])
            flows.loc[(flows['Source ID']==c_start) & (flows['Destination ID']==c_end), 'Path'] = True
    path = flows.loc[flows['Path']==True]
    print(path)
    traffic, tp = sum(flows['Magnitude']), sum(path['Magnitude'])
    print('Traffic proportion for', ranking+':', tp/traffic*100)
    #path.to_file('path_'+ranking+'.geojson', driver = 'GeoJSON')
    #print('Exported path to file path_'+ranking+'.geojson\n')


#==============================================================================
if __name__=='__main__':
    ids_geo = [500, 7, 8, 18, 9, 4, 5, 15, 11, 10, 12, 700]
    ids_ago = [500, 13, 14, 15, 4, 5, 17, 51, 6, 10, 12, 700]
    ids_med = [500, 7, 8, 9, 18, 15, 4, 5, 11, 10, 12, 700]
    export_gjson_path(ids_geo, 'Georank')
    export_gjson_path(ids_ago, 'Agorank')
    export_gjson_path(ids_med, 'Medianrank')
