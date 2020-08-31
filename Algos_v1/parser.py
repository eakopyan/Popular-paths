"""
TODO:
--> Regroup datasets: consider multiple files as a single dataset: DONE
--> Regroup gateways by clusters: consider geographically close GW as a single
    entity: DONE
--> Recompute segments with clustered points.
--> Compute rank (add as property) and derive agony.
--> Recompute segment weight taking agony into account (as coefficient?)
"""

import os
from tqdm import tqdm
from typing import List
from optparse import OptionParser

from dataProcessor import get_path, extract_paths
from geoProcessor import GwPoint, Segment, create_point_map, create_segment_map, to_crs, point_to_segment, compute_segment_weight
from graphProcessor import derive_distances, compute_rank

#------------------------------------------------------------------------------
user_dir_path = "/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/sample1_users/"
maps_dir_path = "/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/maps/"

#------------------------------------------------------------------------------
if __name__ == "__main__":
    # Options parser: process correct number of files
    opt = OptionParser()
    opt.add_option("-r", "--range", type="int", dest="range", help="specify set range: number of files to process")
    (options, args) = opt.parse_args()
    # Options control
    if not options.range:
        print("Must specify range (-r int)")
        exit()

    list_user: List[str] = [] # Contains all file names
    counter = 0
    print('Preparing to process', options.range, 'files')
    for root, dirs, files in os.walk(user_dir_path):
        for f in sorted(files, key = lambda f: int(f[7:f.find(".txt")])):
            counter += 1
            if counter <= options.range:
                list_user.append(f)
            else:
                break

    # Extracting data from files
    trajectory: List['GwPoint'] = []
    with tqdm(total=len(list_user), desc="Extracting paths") as file_bar:
        for file in list_user:
            path_user = get_path(user_dir_path + file)
            for p in extract_paths(path_user):
                trajectory.append(p)
            file_bar.update(1)

    # Formatting coordinates
    with tqdm(total=len(trajectory), desc="CRS conversion") as crs_bar:
        for t in trajectory:
            to_crs(t)
            crs_bar.update(1)

    # Computing rank
    print('Computing base station ranks...')
    (centroids, distances) = derive_distances(trajectory)
    compute_rank(centroids, distances, trajectory)
    print(distances.head())

    # Computing segment weight
    segments = point_to_segment(trajectory)
    compute_segment_weight(segments)

    # Creating Point file
    print("\nCreating GeoJSON Point file from extracted data...")
    gw_map = create_point_map(trajectory, user_dir_path + file)
    print("Exporting file to", maps_dir_path + "points.geojson...")
    GwPoint.export(gw_map, maps_dir_path + "points.geojson")
    #GwPoint.export(gw_map, maps_dir_path + "testpoints.geojson")

    # Creating LineString PATH file
    """print("\nCreating GeoJSON LineString Path file from extracted data...")
    path_map = create_paths_map(trajectory, user_dir_path + file)
    print("Exporting file to", maps_dir_path + "paths.geojson...")
    #Path.export(path_map, maps_dir_path + "paths.geojson")
    Path.export(path_map, maps_dir_path + "testpaths.geojson")"""

    # Creating LineString SEGMENT file
    print("\nCreating GeoJSON LineString Segment file from extracted data...")
    seg_map = create_segment_map(segments, user_dir_path + file)
    print("Exporting file to", maps_dir_path + "segments.geojson...")
    Segment.export(seg_map, maps_dir_path + "segments.geojson")
    #Segment.export(seg_map, maps_dir_path + "testsegments.geojson")
