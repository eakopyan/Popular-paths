import os
from typing import List
from optparse import OptionParser

from dataProcessor import parse_file, parse_basestations, remove_noise, get_observations, to_graph, format_date
from geoProcessor import BSPoint, ArcLine, create_geojson_points, create_geojson_lines

user_dir_path = "/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/sample1_users/"
gjson_dir_path = "/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/GeoJson/"

"""Input data:
    Set of entities E: steps
    Set of observations Obs <v,e,t>: <base station,user,timestamp>
    Directed graph G(V,wA): (base stations, weighted arcs)
"""

if __name__ == "__main__":
    # Options parser: process correct number of files
    opt = OptionParser()
    opt.add_option("-r", "--range", type="int", dest="range", help="specify set range: number of files to process")
    (options, args) = opt.parse_args()
    if not options.range:
        print("Must specify range (-r int)")
        exit()

    list_files: List[str] = [] # Contains all file names
    counter = 0
    for root, dirs, files in os.walk(user_dir_path):
        for f in sorted(files, key = lambda f: int(f[7:f.find(".txt")])):
            counter += 1
            if counter <= options.range:
                list_files.append(f)
            else:
                break

    dataset = parse_basestations() # Whole set of base stations with WGS84 coordinates
    steps = [] # Whole set of entities (E)
    print('Preparing to process', len(list_files), 'files...')
    for file in list_files:
        step = parse_file(user_dir_path + file)
        for s in step:
            steps.append(s)
    print('Done. Number of steps:', len(steps))

    print('\nInitiating noise reduction...')
    paths = remove_noise(steps, 3) # List of DAGs: database of propagation
    print('Done. Number of paths:', len(paths))

    print('\nCreating set of observations...')
    (obs_AtoB, obs_BtoA) = get_observations(dataset, paths) # List of sets of observations
    print('Done. Found', len(obs_AtoB), 'paths from A to B and', len(obs_BtoA), 'paths from B to A.')

    # Deriving directed, weighted union graphs
    print('\nCreating union graph for A-B paths...')
    (base_stations_AtoB, arcs_AtoB) = to_graph(obs_AtoB)
    max_weight = 0
    for arc in arcs_AtoB:
        if arc.weight > max_weight:
            max_weight = arc.weight
    print('Maximum arc weight:', max_weight)

    print('\nCreating union graph for B-A paths...')
    (base_stations_BtoA, arcs_BtoA) = to_graph(obs_BtoA)
    max_weight = 0
    for arc in arcs_BtoA:
        if arc.weight > max_weight:
            max_weight = arc.weight
    print('Maximum arc weight:', max_weight)

    # Computing minimum agony and corresponding ranks
    print('\nComputing minimum agony and corresponding ranks for A-B graph...')
    graph_agony = 0
    for arc in sorted(arcs_AtoB, key = lambda k: k.weight, reverse = True): # Arcs sorted by weight for optimization
        arc.compute_agony(base_stations_AtoB)
        graph_agony += arc.agony
    print('... Done. Graph agony:', graph_agony)

    print('\nComputing minimum agony and corresponding ranks for B-A graph...')
    graph_agony = 0
    for arc in sorted(arcs_BtoA, key = lambda k: k.weight, reverse = True): # Arcs sorted by weight for optimization
        arc.compute_agony(base_stations_BtoA)
        graph_agony += arc.agony
    print('... Done. Graph agony:', graph_agony)


    # Exporting to GeoJSON files
    bs_geojson = create_geojson_points(base_stations_AtoB)
    print('Exporting data to', gjson_dir_path+'pointsAB.geojson...')
    BSPoint.export(bs_geojson, gjson_dir_path + "pointsAB.geojson")
    al_geojson = create_geojson_lines(arcs_AtoB)
    print('Exporting data to', gjson_dir_path+'arclinesAB.geojson...')
    ArcLine.export(al_geojson, gjson_dir_path + "arclinesAB.geojson")

    bs_geojson = create_geojson_points(base_stations_BtoA)
    print('Exporting data to', gjson_dir_path+'pointsBA.geojson...')
    BSPoint.export(bs_geojson, gjson_dir_path + "pointsBA.geojson")
    al_geojson = create_geojson_lines(arcs_BtoA)
    print('Exporting data to', gjson_dir_path+'arclinesBA.geojson...')
    ArcLine.export(al_geojson, gjson_dir_path + "arclinesBA.geojson")
