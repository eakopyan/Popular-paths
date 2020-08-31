import csv
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
from typing import List, Dict


class Step:
    def __init__(self, user_id, timestamp, bs_id, lon, lat, zone):
        self.user_id = user_id
        self.timestamp = timestamp
        self.bs_id = bs_id
        self.lon = lon
        self.lat = lat
        self.zone = zone[0]

    def __str__(self):
        return self.user_id+"\tDate: "+self.timestamp+"\tBaseStation ID:"+str(self.bs_id)+" Zone "+self.zone


class Place: # Node P
    def __init__(self, id, longitude, latitude, zone, tsp):
        self.id = id # (CI, LAC)
        self.lon = float(longitude)
        self.lat = float(latitude)
        self.zone = zone[0]
        self.timestamp = tsp
        self.cluster_id = -1
        self.rank = -1.0

    def __str__(self):
        return "Place ID "+str(self.id)+" Coord: "+str(self.lon)+", "+str(self.lat)+" Zone "+self.zone+"\tDate: "+self.timestamp

class ClusterPlace:
    def __init__(self, id, longitude, latitude, weight):
        self.id = id
        self.lon = float(longitude)
        self.lat = float(latitude)
        self.weight = weight
        self.place_ids = []
        self.rank = []

    def __str__(self):
        return "Cluster ID: "+self.id+" Coord: "+str(self.lon)+", "+str(self.lat)+"\tWeight: "+str(self.weight)


class Flow: # Directed connection between places A and B
    def __init__(self, user_id, id_first, id_last, coord_first, coord_last, tsp_first, tsp_last):
        self.user_id = user_id
        self.id_first = id_first # (CI, LAC)
        self.id_last = id_last # (CI, LAC)
        self.coord_first = coord_first
        self.coord_last = coord_last
        self.timestamp_first = tsp_first # Time step of departure
        self.timestamp_last = tsp_last # Time step of arrival (can be equal to departure)

    def __str__(self):
        return self.user_id+": Flow from Place "+str(self.id_first)+" to "+str(self.id_last)+"\t"+self.timestamp_first.__str__()

    def mvt_equals(self, flow):
        if (self.id_first, self.id_last) == (flow.id_first, flow.id_last):
            return True
        return False

    def get_place(self, places: List['Place'], pos: str):
        for p in places:
            if pos == 'first':
                if p.id == self.id_first:
                    return p
            if pos == 'last':
                if p.id == self.id_last:
                    return p


class ClusterFlow: # Flow between clustered places
    def __init__(self, id_first, id_last, coord_first, coord_last, mag):
        self.id_first = id_first # ID of cluster
        self.id_last = id_last
        self.coord_first = coord_first
        self.coord_last = coord_last
        self.magnitude = mag

    def __str__(self):
        return "Flow from Cluster "+str(self.id_first)+" to "+str(self.id_last)+"\tMagnitude: "+str(self.magnitude)


class Timestep: # Time interval
    def __init__(self, time1, time2):
        self.begin = datetime.strptime(time1, '%Y-%m-%d %H:%M:%S')
        self.end = datetime.strptime(time2, '%Y-%m-%d %H:%M:%S')
        self.delta = self.end-self.begin # Type timedelta

    def __str__(self):
        return "Begin: "+self.begin.ctime()+" End: "+self.end.ctime()+"\tDelta: "+str(self.delta)

    def greater_than(self, seconds: int):
        if self.delta > timedelta(0, seconds):
            return True
        return False


#------------------------------------------------------------------------------
def parse_file(filename: str, dataset):
    path: List['Step'] = []
    with open(filename) as f:
        with tqdm(desc='Parsing file') as pbar:
            for line in f.readlines():
                elem = line.split(";")
                (bs_ci, bs_lac) = (elem[3], elem[2])
                bs_id = (bs_ci[:bs_ci.find('\n')], elem[2])
                data = get_place_data(bs_id, dataset)
                if len(data) > 3:
                    path.append(Step(
                        elem[0], # User ID
                        elem[1], # Timestamp
                        bs_id, # (CI, LAC)
                        data[1], # Longitude
                        data[2], # Latitude
                        data[3] # Zone
                    ))
                pbar.update(1)
    return path

def parse_places(sorted_paths: Dict[str, List[List['Step']]]):
    places: List['Place'] = []
    for uid, paths in sorted_paths.items():
        for path in paths:
            for i, step in enumerate(path):
                place = Place(
                    step.bs_id,
                    step.lon,
                    step.lat,
                    step.zone,
                    step.timestamp
                )
                place.rank = int(i/(len(path)-1)*100)
                places.append(place)
    return places

def parse_flows(sorted_paths: Dict[str, List[List['Step']]]):
    flows: List[List['Flow']] = []
    for uid, paths in sorted_paths.items():
        for path in paths:
            current_flow: List['Flow'] = []
            for idx, step in enumerate(path):
                if idx+1 < len(path):
                    next = path[idx+1]
                    current_flow.append(Flow(
                        step.user_id,
                        step.bs_id, # First (CI, LAC)
                        next.bs_id, # Last (CI, LAC)
                        (step.lon, step.lat), # Coordinates of first
                        (next.lon, next.lat), # Coordinates of last
                        step.timestamp, # Time of departure
                        next.timestamp # Time of arrival
                    ))
            if len(current_flow) > 0:
                flows.append(current_flow)
    return flows

def preprocess_data(steps: List['Step'], thresh, filter=True):
    sorted_steps: Dict[str, List['Step']] = separate_users(steps)
    sorted_paths: Dict[str, List[List['Step']]] = {}
    nb_paths, nb_ab_paths, nb_steps = 0, 0, 0
    for uid, path in sorted_steps.items():
        paths: List[List['Step']] = check_time(path, thresh)
        nb_paths += len(paths)
        sorted_paths[uid] = separate_paths(paths)
        nb_ab_paths += len(sorted_paths[uid])
        for path_set in sorted_paths[uid]:
            nb_steps += len(path_set)
    print('Time threshold', thresh, 'minutes:', nb_paths, 'paths')
    print('Number of AB paths:', nb_ab_paths, '\tNumber of AB places:', nb_steps)
    if filter:
        count_dag = 0
        with tqdm(total=len(sorted_paths), desc='Removing cycles') as pbar:
            for uid, paths in sorted_paths.items():
                for id, path in enumerate(paths):
                    paths[id] = to_DAG(path)
                sorted_paths[uid] = paths
                for path in sorted_paths[uid]:
                    count_dag += len(path)
                pbar.update(1)
        print('Number of places after cycle reduction:', count_dag)
    return sorted_paths

def remove_nan(path: List['Step'], dataset): # OBSOLETE
    for step in path:
        if get_place_data(step.bs_id, dataset) == 'NaN':
            path.remove(step)
            path = remove_nan(path, dataset)
    return path

def separate_paths(paths: List[List['Step']]):
    dag_paths: List[List['Step']] = []
    for path_set in paths:
        (src, dest) = find_endpoints(path_set)
        dest_index = 0
        while 'D' not in (src, dest): # Loop until EOF
            for s in path_set[dest_index:]:
                dest_index += 1
                if s.zone == dest:
                    break
            src_index = dest_index
            for s in reversed(path_set[:dest_index]):
                src_index -= 1
                if s.zone == src:
                    break
            dag_paths.append(path_set[src_index:dest_index])
            (src, dest) = find_endpoints(path_set[dest_index:])
    return dag_paths

def check_time(steps: List['Step'], thresh):
    paths: List[List['Step']] = []
    current_path: List['Step'] = []
    for idx, step in enumerate(steps):
        current_path.append(step)
        if idx+1 < len(steps):
            timestep = Timestep(step.timestamp, steps[idx+1].timestamp)
            if timestep.greater_than(thresh*60):
                paths.append(current_path)
                current_path = []
    paths.append(current_path)
    return paths

def find_endpoints(path: List['Step']):
    src = 'D'
    dest = 'D'
    index = 0
    for s in path:
        if s.zone != 'C':
            src = s.zone
            break
        index += 1
    for s in path[index:]:
        if s.zone not in (src, 'C'):
            dest = s.zone
            break
    return (src, dest)

def to_DAG(path: List['Step']):
    dag = path
    visited = [] # List of IDs of visited points
    for d in dag:
        if d.bs_id not in visited:
            visited.append(d.bs_id)
        else:
            dag = remove_cycle(dag, d.bs_id)
            dag = to_DAG(dag)
            break
    return dag

def remove_cycle(path: List['Step'], id):
    idx_start, idx_end = 0,0
    for i, step in enumerate(path):
        if step.bs_id == id:
            idx_start = i+1
            break
    for j, step in reversed(list(enumerate(path))):
        if step.bs_id == id:
            idx_end = j+1
            break
    dag = path[:idx_start]+path[idx_end:]
    return dag

def separate_users(steps: List['Step']):
    sorted_users = {}
    for step in steps:
        if step.user_id not in sorted_users:
            sorted_users[step.user_id] = [step]
        else:
            sorted_users[step.user_id].append(step)
    for uid, path in sorted_users.items():
        sorted_users[uid] = sorted(path, key = lambda k: k.timestamp)
    return sorted_users

def get_people_count(places: List['Place']):
    pcount = {} # ID, weight
    print('Setting weight...')
    for place in places:
        if place.id in pcount:
            pcount[place.id] += 1
        else:
            pcount[place.id] = 1
    ranks = {} # ID, ranks
    print('Setting ranks...')
    for place in places:
        if place.id in ranks:
            ranks[place.id].append(place.rank)
        else:
            ranks[place.id] = [place.rank]
    return [pcount, ranks]

def get_flow_magnitude(flows: List[List['Flow']]):
    fmag = {} # (bs_id1, bs_id2), weight
    for user_flow in flows:
        for flow in user_flow:
            if (flow.id_first, flow.id_last) in fmag:
                fmag[(flow.id_first, flow.id_last)] += 1
            else:
                fmag[(flow.id_first, flow.id_last)] = 1
    return fmag

def get_supergraph(paths: Dict[str, List[List['Step']]]):
    places: List['Place'] = parse_places(paths)
    flows: List[List['Flow']] = parse_flows(paths)
    return [places, flows]

def parse_basestations(file):
    dataset = []
    with open(file, "r") as f:
        csv_reader = csv.reader(f, delimiter = ',')
        for line in csv_reader:
            if line[0] == 'ci': # Title line
                pass
            else:
                dataset.append([
                    (line[0], line[1]), # (ID, LAC)
                    float(line[3]), # Longitude
                    float(line[2]), # Latitude
                    line[4] # Zone
                ])
    return dataset

def get_place_data(id, dataset):
    for line in dataset:
        if line[0] == id: # Found match
            return line
    return 'NaN'
