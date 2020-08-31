import geopandas as gpd
import pandas as pd
import networkx as nx
import numpy as np
import scipy as sp
from itertools import islice
from networkx.algorithms import approximation
from tqdm import tqdm

from analyser import compare_paths, get_traffic_proportion, get_id_traffic_proportion
from geoProcessor import *
from paths import export_path

flows_path="../data/flows.geojson"
medianpoints_path="../data/points_medianrank.geojson"
agopoints_path="../data/points_agorank.geojson"
geopoints_path="../data/points_georank.geojson"
gt_path="../data/GT_WGS84.geojson"
id_flows_path="/home/agora/Documents/Popular_paths/Data/GeoJson/SFO_30_clusterflows.geojson"
id_places_path="/home/agora/Documents/Popular_paths/Data/GeoJson/SFO_30_clusterplaces.geojson"


#===============================================================================
def compute_geo(flows):
    points = gpd.GeoDataFrame.from_file(geopoints_path)
    print(points.head())
    maxpath = compute_maxcost(points, flows, 'Georank')
    print('Maximum cost path by geographic ranking:')
    print(maxpath)

def to_pandas(data, alpha):
    df = data.iloc[:, 0:3]
    pmin, pmax = np.amin(df['Weight']), np.amax(df['Weight'])
    #print('Weight range:', [pmin, pmax])
    #print('Total traffic:', np.sum(df['Weight']))
    #df['Score'] = 1 + alpha*((pmax-df['Weight'])/(pmax-pmin))
    df['Score'] = pow(pmin/df['Weight'], alpha)
    #df['Score'] = (df['Weight']/sp.linalg.norm(df['Weight']))*alpha
    #df['Score'] = pmin/pow(df['Weight'], alpha)
    return df

def find_maxpath(path_ids, data):
    data['Path'] = False
    for i, n in enumerate(path_ids):
        if i+1 < len(path_ids):
            c_start, c_end = n, path_ids[i+1]
            data.loc[(data['c_start']==c_start) & (data['c_end']==c_end), 'Path'] = True
    path = data.loc[data['Path']==True]
    return path

def find_id_maxpath(path_ids, data):
    data['Path'] = False
    for i, n in enumerate(path_ids):
        if i+1 < len(path_ids):
            c_start, c_end = n, path_ids[i+1]
            data.loc[(data['Source ID']==str(c_start)) & (data['Destination ID']==str(c_end)), 'Path'] = True
    path = data.loc[data['Path']==True]
    return path

def to_nx_graph(data, points, ranking):
    alpha = 340
    if ranking == 'Agorank':
        alpha = 1800
    print('Alpha =', alpha)
    df = to_pandas(data, alpha)
    mdg = nx.from_pandas_edgelist(df, source='c_start', target='c_end', edge_attr='Score', create_using=nx.DiGraph())
    top5 = list(islice(nx.shortest_simple_paths(mdg, source=153, target=6, weight='Score'), 5))
    gt_ids = []
    for maxpath in top5:
        print(maxpath)
        path = find_maxpath(maxpath, data)
        gt_ids.extend(compare_paths(path))
        get_traffic_proportion(path, data, ranking)
    print_top5(top5, data, points, ranking)
    #[mc_path, ml_path, gt_path] = analyse_top5(top5, data, ranking)
    #create_comparative_map(mc_path, ml_path, gt_path, points, ranking)

def analyse_top5(top5, data, ranking):
    maxcost_path, ml_path = (), () # Path ID, weight/GT path ID
    maxcost, mindist = 0, 100
    for i, path_ids in enumerate(top5):
        path = find_maxpath(path_ids, data)
        if sum(path['Weight']) > maxcost:
            maxcost = sum(path['Weight'])
            maxcost_path = (i, sum(path['Weight']))
        compare = compare_paths(path)
        dist, gt_id = compare[0], compare[1]
        if dist < mindist:
            mindist = dist
            ml_path = (i, gt_id)
    print('Mindist path ID:', ml_path[0], '\tGT path ID:', ml_path[1])
    print('Maxcost path ID:', maxcost_path[0], '\tWeight:', maxcost_path[1])
    #get_traffic_proportion(find_maxpath(top5[maxcost_path[0]], data), data, ranking)
    gt = gpd.GeoDataFrame.from_file(gt_path)
    return [find_maxpath(top5[maxcost_path[0]], data), find_maxpath(top5[ml_path[0]], data), gt.loc[gt['ID']==str(ml_path[1])]]

def analyse_id_top5(top5, data, ranking):
    maxcost_path, ml_path = (), () # Path ID, weight/GT path ID
    maxcost, mindist = 0, 100
    for i, path_ids in enumerate(top5):
        path = find_id_maxpath(path_ids, data)
        if sum(path['Magnitude']) > maxcost:
            maxcost = sum(path['Magnitude'])
            maxcost_path = (i, sum(path['Magnitude']))
        compare = compare_paths(path)
        dist, gt_id = compare[0], compare[1]
        if dist <= mindist:
            mindist = dist
            ml_path = (i, gt_id)
    print('Mindist path ID:', ml_path[0], '\tGT path ID:', ml_path[1])
    print('Maxcost path ID:', maxcost_path[0], '\tWeight:', maxcost_path[1])
    get_id_traffic_proportion(find_id_maxpath(top5[maxcost_path[0]], data), data, 'Maxcost path')
    get_id_traffic_proportion(find_id_maxpath(top5[ml_path[0]], data), data, 'ML path')
    gt = gpd.GeoDataFrame.from_file(gt_path)
    return [find_id_maxpath(top5[maxcost_path[0]], data), find_id_maxpath(top5[ml_path[0]], data), gt.loc[gt['ID']==str(ml_path[1])]]

def create_comparative_map(mc_path, ml_path, gt_path, points, ranking):
    map = create_map()
    add_enb(points, map, 'Clusters', rank=ranking, emphasis=False)
    add_path(gt_path, map, 'GT path', color='#505050', gt=True)
    add_path(mc_path, map, 'Maxcost path', color='#BB0000')
    add_path(ml_path, map, 'ML path', color='#001DBB')
    close_map(map, "Compare_HO_"+ranking+".html")

def create_comparative_id_map(mc_path, ml_path, gt_path, points, ranking):
    map = create_map()
    add_places(points, map, 'Clusters', rank=ranking)
    add_path(gt_path, map, 'GT path', color='#505050', gt=True)
    add_id_path(mc_path, map, 'Maxcost path', color='#BB0000')
    add_id_path(ml_path, map, 'ML path', color='#001DBB')
    close_map(map, "Compare_ID_"+ranking+".html")

def print_top5(paths, flows, points, ranking):
    colors = ['#FFD700', '#FFB90F', '#FFA010', '#FF6000', '#BB0000']
    colors = colors[len(colors)-len(paths):]
    print('Creating map...')
    map = create_map()
    #add_flows(flows, map, 'Handovers')
    add_enb(points, map, 'Clusters', rank=ranking, emphasis=False)
    paths = order_paths(paths, flows)
    for pos, maxpath in enumerate(paths):
        path = find_maxpath(maxpath, flows)
        add_path(path, map, 'Popular path '+str((5-pos)), color=colors[pos])
    close_map(map, "Top5_HO_"+ranking+".html")

def process_id_dataset():
    top5_geo = [
        [500, 14, 18, 9, 4, 5, 15, 44, 11, 10, 12, 700],
        [500, 14, 18, 9, 4, 5, 15, 11, 10, 12, 700],
        [500, 14, 18, 9, 4, 5, 15, 17, 11, 10, 12, 700],
        [500, 13, 14, 18, 9, 4, 5, 15, 11, 10, 12, 700],
        [500, 7, 8, 18, 9, 4, 5, 15, 11, 10, 12, 700]
    ]
    top5_ago = [
        [500, 14, 15, 4, 5, 18, 21, 6, 10, 12, 700],
        [500, 14, 15, 4, 5, 18, 21, 6, 10, 11, 12, 700],
        [500, 14, 15, 4, 5, 18, 20, 21, 6, 10, 12, 700],
        [500, 13, 14, 15, 4, 5, 17, 6, 10, 11, 12, 700],
        [500, 13, 14, 15, 4, 5, 17, 51, 6, 10, 12, 700]
    ]
    top5_med = [
        [500, 14, 9, 18, 15, 4, 5, 17, 11, 10, 12, 700],
        [500, 19, 9, 18, 15, 4, 5, 17, 11, 10, 12, 700],
        [500, 13, 19, 9, 18, 15, 4, 5, 11, 10, 12, 700],
        [500, 13, 14, 9, 18, 15, 4, 5, 11, 10, 12, 700],
        [500, 7, 8, 9, 18, 15, 4, 5, 11, 10, 12, 700]
    ]
    flows_df = gpd.GeoDataFrame.from_file(id_flows_path)
    places_df = gpd.GeoDataFrame.from_file(id_places_path)
    colors = ['#FFD700', '#FFB90F', '#FFA010', '#FF6000', '#BB0000']

    [mc_path, ml_path, gt_path] = analyse_id_top5(top5_geo, flows_df, 'Georank')
    create_comparative_id_map(mc_path, ml_path, gt_path, places_df, 'Georank')
    print('\n')
    """map = create_map()
    add_places(places_df, map, 'Clusters', rank='Georank')
    for pos, maxpath in enumerate(top5_geo):
        path = find_id_maxpath(maxpath, flows_df)
        add_id_path(path, map, 'Popular path '+str((5-pos)), color=colors[pos])
    close_map(map, "Top5_ID_Georank.html")"""
    [mc_path, ml_path, gt_path] = analyse_id_top5(top5_ago, flows_df, 'Agorank')
    create_comparative_id_map(mc_path, ml_path, gt_path, places_df, 'Agorank')
    print('\n')
    [mc_path, ml_path, gt_path] = analyse_id_top5(top5_med, flows_df, 'Medianrank')
    create_comparative_id_map(mc_path, ml_path, gt_path, places_df, 'Medianrank')



def order_paths(path_ids, flows):
    corresp = {} # Path ID, weight
    order, weights, ordered_paths = [], [], []
    for pos, maxpath in enumerate(path_ids):
        path = find_maxpath(maxpath, flows)
        weights.append(sum(path['Weight']))
        corresp[pos] = weights[-1]
    weights = list(sorted(weights))
    for i, w in enumerate(weights):
        for k,v in corresp.items():
            if v == w:
                order.append(k)
    for p in order:
        ordered_paths.append(path_ids[p])
    return ordered_paths

def export_alpha(alpha, dist):
    df = pd.DataFrame(data={'Alpha':alpha, 'Min distance':dist})
    df.to_csv('../data/dist_alpha.csv', sep=';', index=False)
    print('Exported alpha in file dist_alpha.csv')

def drop_noise(flows, points, ranking):
    # Removing weak links
    weak = flows.loc[flows['Weight'] < np.quantile(flows['Weight'], 0.75)].index
    flows.drop(weak, inplace=True)
    # Removing over-the-top rank clusters
    r_max = points.loc[points['Cluster']==6, ranking].values[0]
    points.drop(points.loc[points[ranking] > r_max].index, inplace=True)
    out_of_range = flows.loc[(~flows['c_end'].isin(points['Cluster'])) | (~flows['c_start'].isin(points['Cluster']))]
    flows.drop(out_of_range.index, inplace=True)
    # Removing agonizing links
    flows['Agony'] = False
    for idx, row in flows.iterrows():
        rank_src = points.loc[points['Cluster']==row['c_start'], ranking].values[0]
        rank_dest = points.loc[points['Cluster']==row['c_end'], ranking].values[0]
        if rank_src >= rank_dest:
            flows.loc[idx,'Agony'] = True
    flows.drop(flows.loc[flows['Agony']==True].index, inplace=True)

#===============================================================================
if __name__=='__main__':
    #flows = gpd.GeoDataFrame.from_file(flows_path)
    #ranking = 'Georank'
    #points = gpd.GeoDataFrame.from_file(geopoints_path)
    #points = gpd.GeoDataFrame.from_file(medianpoints_path)
    #points = gpd.GeoDataFrame.from_file(agopoints_path)
    #drop_noise(flows, points, ranking)
    #maxpath = to_nx_graph(flows, points, ranking)
    #export_path(maxpath, flows, points, 'Medianrank')

    process_id_dataset()
