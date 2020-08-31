import csv
import datetime
import numpy as np
from typing import List
from scipy.spatial import distance


class BaseStation: # G(V,wA): nodes V
    def __init__(self, id, longitude, latitude, zone):
        self.id = id
        self.lon = longitude
        self.lat = latitude
        self.zone = zone
        self.rank = []
        self.dist = 0.0

    def __str__(self):
        return "BaseStation ID: "+str(self.id)+"\tCoordinates: "+str(self.lon)+" ; "+str(self.lat)+"\tRank: "+str(self.rank)

    def in_set(self, set):
        for s in set:
            if s.id == self.id:
                return True
        return False

    def get_step_by_id(self, steps): # Return first step found
        for s in steps:
            if s.id == self.id:
                return s
            else:
                pass

    def get_indexes(self, observations: List[List['Observation']]):
        indexes = []
        for obs_set in observations:
            for idx, obs in enumerate(obs_set):
                if obs.id == self.id:
                    indexes.append(idx)
        return indexes

    def set_distance(self, centroids):
        dist = distance.euclidean([self.lon, self.lat], centroids[0]) # Distance wrt centroid A
        dist += distance.euclidean([self.lon, self.lat], centroids[1]) # Add distance wrt centroid B
        self.dist = dist


class Arc: # G(V,wA): weighted arcs wA
    def __init__(self, first, last, coord_first, coord_last):
        self.first = first # IDs
        self.last = last # IDs
        self.coord_first = coord_first
        self.coord_last = coord_last
        self.deltatime = []
        self.weight = 1
        self.agony = 0

    def __str__(self):
        return "Arc between BS "+str(self.first)+" and "+str(self.last)+"\tWeight: "+str(self.weight)+"\tAgony: "+str(self.agony)

    def increment(self):
        self.weight += 1

    def in_set(self, set):
        for a in set:
            if (a.first, a.last) == (self.first, self.last):
                return True
        return False

    def get_bs_by_id(self, base_stations: List['BaseStation'], pos: str):
        for bs in base_stations:
            if pos == 'first':
                if bs.id == self.first:
                    return bs
            if pos == 'last':
                if bs.id == self.last:
                    return bs

    def get_average_duration(self): # Returns average travel time in seconds
        deltas = []
        for delta in self.deltatime:
            deltas.append(delta.seconds)
        array = np.array(deltas)
        return np.mean(array)

    def compute_agony(self, base_stations: List['BaseStation']):
        min_agony = 1000000
        rank_first, rank_last = 0, 0
        bs_first = self.get_bs_by_id(base_stations, 'first')
        bs_last = self.get_bs_by_id(base_stations, 'last')
        for rf in bs_first.rank:
            for rl in bs_last.rank:
                agony = self.weight * (rf*bs_first.dist - rl*bs_last.dist + 1)
                if max(agony,0) < min_agony: # Don't allow negative agony
                    min_agony = max(agony,0)
                    rank_first = [rf]
                    rank_last = [rl]
        bs_first.rank = rank_first
        bs_last.rank = rank_last
        self.agony = min_agony


class Step:
    def __init__(self, id, zone, timestamp):
        self.id = id
        self.zone = zone[0]
        self.timestamp = timestamp

    def __str__(self):
        return "Step ID: "+str(self.id)+"\tZone: "+self.zone+"\tDate: "+self.timestamp

    def get_bs_by_id(self, dataset):
        for bs in dataset:
            if bs.id == self.id:
                return bs
            else:
                pass


class Observation:
    def __init__(self, id, longitude, latitude, zone, timestamp):
        self.id = id # Entity
        self.lon = longitude # Node
        self.lat = latitude # Node
        self.zone = zone # Entity
        self.timestamp = self.format_date(timestamp) # Time

    def __str__(self):
        return "Observed user at BS "+str(self.id)+" (coord "+str(self.lon)+" "+str(self.lat)+", zone "+self.zone+") at time "+self.timestamp.ctime()

    def format_date(self, datestring):
        return datetime.datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')


#------------------------------------------------------------------------------
def parse_file(filename: str): # DONE
    path: List['Step'] = []
    with open(filename) as f:
        for line in f.readlines():
            data = line.split(",")
            path.append(Step(
                (data[2], data[1]), # (ID, LAC)
                data[3], # Zone
                data[0], # Timestamp
            ))
    return path


def get_base_stations(observations: List[List['Observation']]): # DONE: unweighted (no doubles)
    basestations: List['BaseStation'] = []
    for obs_set in observations:
        for obs in obs_set:
            bs = BaseStation(
                obs.id,
                obs.lon,
                obs.lat,
                obs.zone
            )
            if not bs.in_set(basestations):
                basestations.append(bs)
    for bs in basestations:
        bs.rank = bs.get_indexes(observations)
    return basestations


def get_arcs(observations: List[List['Observation']]): # DONE: weighted
    arcs: List['Arc'] = []
    for obs_set in observations:
        for idx, obs in enumerate(obs_set):
            if idx+1 != len(obs_set): # Doesn't reach end of list
                arc = Arc(
                    obs.id,
                    obs_set[idx+1].id,
                    (obs.lon, obs.lat),
                    (obs_set[idx+1].lon, obs_set[idx+1].lat)
                )
                delta = obs_set[idx+1].timestamp - obs.timestamp
                if arc.in_set(arcs):
                    target = find_arc(arcs, arc)
                    target.increment()
                    target.deltatime.append(delta)
                else:
                    arc.deltatime.append(delta)
                    arcs.append(arc)
    return arcs


def remove_noise(steps: List['Step'], min_length=1): # DONE
    paths: List[List['Step']] = []
    steps = sorted(steps, key = lambda k: k.timestamp)
    (src, dest) = find_endpoints(steps)
    dest_index = 0
    while 'D' not in (src, dest): # Loop until the end of the file is reached
        for s in steps[dest_index:]:
            dest_index += 1
            if s.zone == dest:
                break
        src_index = dest_index
        for s in reversed(steps[:dest_index]):
            src_index -= 1
            if s.zone == src:
                break
        dag = to_DAG(steps[src_index:dest_index])
        paths.append(dag)
        src_index = dest_index
        (src, dest) = find_endpoints(steps[src_index:])
    paths = remove_irrelevant(paths, min_length)
    return paths


def get_observations(dataset: List['BaseStation'], paths: List[List['Step']]): # DONE
    obs_AtoB: List[List['Observation']] = [] # From St-Foy to Part-Dieu
    obs_BtoA: List[List['Observation']] = [] # From Part-Dieu to St-Foy
    for path in paths:
        obs: List['Observation'] = []
        fromAtoB = True
        if find_endpoints(path) == ('B','A'): # Reversed path
            fromAtoB = False
        for p in path:
            obs.append(Observation(
                p.id,
                p.get_bs_by_id(dataset).lon,
                p.get_bs_by_id(dataset).lat,
                p.zone,
                p.timestamp
            ))
        if fromAtoB:
            obs_AtoB.append(obs)
        else:
            obs_BtoA.append(obs)
    return (obs_AtoB, obs_BtoA)


def to_graph(observations: List[List['Observation']]): # DONE: graph G(V, wA)
    base_stations: List['BaseStation'] = [] # Set of nodes
    arcs: List['Arc'] = [] # Set of weighted arcs
    print('Extracting base stations...')
    base_stations = get_base_stations(observations)
    print('Done. Number of base stations:', len(base_stations))
    compute_distances(base_stations)
    print('Extracting arcs...')
    arcs = get_arcs(observations)
    print('Done. Number of arcs:', len(arcs))
    return (base_stations, arcs)


def compute_distances(base_stations: List['BaseStation']): # DONE
    (centrA, centrB) = derive_centroids(base_stations) # Derive centroids
    for bs in base_stations:
        bs.set_distance((centrA, centrB)) # Compute distance of each point


def derive_centroids(base_stations: List['BaseStation']): # DONE
    pointsA, pointsB = [], []
    for bs in base_stations:
        if bs.zone == 'A':
            pointsA.append([bs.lon, bs.lat])
        if bs.zone == 'B':
            pointsB.append([bs.lon, bs.lat])
    pA, pB = np.array(pointsA), np.array(pointsB)
    centrA = [np.mean(pA[:,0]), np.mean(pA[:,1])]
    centrB = [np.mean(pB[:,0]), np.mean(pB[:,1])]
    return (centrA, centrB)


def find_arc(arcs: List['Arc'], arc):
    for a in arcs:
        if (a.first, a.last) == (arc.first, arc.last):
            return a


def remove_irrelevant(dataset: List[List['Step']], min_length):
    paths: List[List['Step']] = []
    for path in dataset:
        if len(path) >= min_length:
            paths.append(path)
    return paths


def to_DAG(steps: List['Step']):
    dag = steps
    loop_bs = [] # Contains a list of all BS, only once
    for d in dag:
        if d.id not in loop_bs:
            loop_bs.append(d.id)
        else: # There is a cycle
            dag = remove_loop(dag, d.id)
            dag = to_DAG(dag)
            break
    return dag


def remove_loop(steps: List['Step'], id):
    first, last = 0, 0
    for i, s in enumerate(steps):
        if s.id == id:
            first = i+1
            break
    for j, s in reversed(list(enumerate(steps))):
        if s.id == id:
            last = j+1
            break
    dag = steps[:first] + steps[last:]
    return dag


def parse_basestations():
    dataset: List['BaseStation'] = []
    with open("/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/cells_wgs84.csv", "r") as f:
        csv_reader = csv.reader(f, delimiter = ';')
        for line in csv_reader:
            if line[0] == 'ci': # Title line
                pass
            else:
                dataset.append(BaseStation(
                    (line[0], line[1]), # (ID, LAC)
                    float(line[2]), # Longitude
                    float(line[3]),  # Latitude
                    'D' # Uninitialized zone
                ))
    return dataset


def find_endpoints(steps: List['Step']):
    src = 'D'
    dest = 'D'
    index = 0
    for s in steps:
        if s.zone != 'C':
            src = s.zone
            break
        index += 1
    for s in steps[index:]:
        if s.zone not in (src, 'C'):
            dest = s.zone
            break
    return (src, dest)


def format_date(delta):
    hours = int(delta/3600)
    minutes = int(delta/60 - hours*60)
    seconds = int(delta) - minutes*60 - hours*60*60
    return str(hours)+" hour(s) "+str(minutes)+" minutes "+str(seconds)+" seconds"
