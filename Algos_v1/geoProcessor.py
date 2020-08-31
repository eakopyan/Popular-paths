import errno
import os
import geojson
import geopandas as gpd
from pyproj import Proj, transform
from geojson import Point, LineString, Feature, FeatureCollection
from typing import List

lambert = Proj('epsg:27572')
wgs84 = Proj('epsg:4326')

#--------------------------- FEATURE PROCESSING -------------------------------

def to_crs(point: 'GwPoint'):
    """Function that transforms coordinates from Lambert II coordinate system
    to WGS-84 (see pyproj.transform for more information).

    Parameters
    ----------
    point: GwPoint
        Target point with obsolete coordinate system
    """

    x,y = point.longitude, point.latitude
    point.longitude, point.latitude = transform(lambert, wgs84, x, y)


def find_endpoints(measures: List['UserMeasure']):
    """Function that parses dataset to derive the source of the path (A or B)
    and the destination. If destination is D, then the parser reached the end
    of the dataset.

    Parameters
    ----------
    measures: List[UserMeasure]
        Dataset to be parsed

    Returns
    -------
    (src, dest): (str, str)
        Source and destination zones
    """

    src = 'D'
    dest = 'D'
    index = 0
    for m in measures:
        if m.zone != 'C':
            src = m.zone
            break
        index += 1
    for m in measures[index:]:
        if m.zone not in (src, 'C'):
            dest = m.zone
            break
    return (src, dest)


def point_to_segment(gateways: List['GwPoint']):
    """Function to extract segments between points out of a point list. Direct
    trajectories between A and B (i.e. without transition points) are not
    taken into account.

    Parameters
    ----------
    gateways : List[GwPoint]
        Point list to parse

    Returns
    -------
    segments : List[Segment]
        List of segments between points
    """

    segments: List['Segment'] = []
    hist = [] # Keep track of existing segments by ids
    for idx, gw in enumerate(gateways):
        if idx+1 == len(gateways): # Reaching end of data
            pass
        elif gw.zone in ('A', 'B') and gateways[idx+1].zone in ('A','B'): # Separation between two paths, or direct path
            pass
        else:
            id1, id2 = (gw.id, gw.lac), (gateways[idx+1].id, gateways[idx+1].lac)
            coord1, coord2 = (gw.longitude, gw.latitude), (gateways[idx+1].longitude, gateways[idx+1].latitude)
            if (id1, id2) not in hist or (id2, id1) not in hist: # New segment
                segments.append(Segment(
                    id1,
                    id2,
                    coord1,
                    coord2,
                    (gw.rank + gateways[idx+1].rank)/2
                ))
                hist.append((id1, id2))
    return segments


def compute_segment_weight(segments: List['Segment']):
    """Function to compute the weight of each segment in a segment list, and
    print out the maximum weight. Direction ((A,B) or (B,A)) is not taken into
    account.

    Parameters
    ----------
    segments : List[Segment]
        List of segments, unweighted
    """

    weights = {} # Key=(id1, id2), value=weight
    print('Compute segments weight...')
    for s in segments:
        point1 = s.id1
        point2 = s.id2
        if (point1, point2) not in weights:
            if (point2, point1) not in weights:
                weights[(point1, point2)] = 1 # Add new segment
            else:
                weights[(point2, point1)] += 1 # Increment reversed segment
        else:
            weights[(point1, point2)] += 1
    max_weight = 0
    for tuple, w in weights.items():
        for s in segments:
            point1 = s.id1
            point2 = s.id2
            if (point1, point2) == tuple or (point2, point1) == tuple:
                s.set_weight(w)
                if w > max_weight:
                    max_weight = w
    print('Maximum weight:', max_weight)


#------------------------------- MAPPING --------------------------------------

class GwPoint:
    def __init__(self, id: str, lac: str, azm: int, x: float, y: float, zone: str, tsp: str):
        self.id = id
        self.lac = lac
        self.azimuth = azm
        self.longitude = x
        self.latitude = y
        self.zone = zone
        self.timestamp = tsp
        self.rank = 0

    def __str__(self):
        res = 'ID: '+self.id+' LAC: '+self.lac+' Azimuth: '+self.azimuth+' Date: '+self.timestamp
        return res

    def set_rank(self, r):
        self.rank = r

    def get_geojson_point(self):
        return Point((self.latitude, self.longitude)) # Keep reversed for folium

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_point(),
            properties = {
                'ID': self.id,
                'LAC': self.lac,
                'Zone': self.zone,
                'Azimuth': self.azimuth,
                'Timestamp': self.timestamp,
                'Rank': self.rank
                }
        )

    @staticmethod
    def get_geojson_feature_collection(dataPoints: List['GwPoint']):
        return FeatureCollection([d.get_geojson_feature() for d in dataPoints])

    @staticmethod
    def export(collection: FeatureCollection, filename: str):
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        with open(filename, "w") as f:
            geojson.dump(collection, f)
        print("Point export completed.\n")


class Path:
    def __init__(self, id: int, src: str, dest: str, gatewayPoints: List['GwPoint']):
        self.id = id
        self.src = src
        self.dest = dest
        self.points = gatewayPoints

    def print_path(self):
        print(
            'Path ID:', self.id,
            'from', self.src,
            'to', self.dest,
            '\tPath length:', len(self.points)-1
        )
        for p in self.points:
            print(p)

    def get_geojson_geometry(self):
        coordinates = []
        for p in self.points:
            coordinates.append((p.latitude, p.longitude)) # Keep reversed for folium
        return LineString(coordinates)

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_geometry(),
            properties = {
                'ID': self.id,
                'Origin': self.src,
                'Destination': self.dest,
                'Length': len(self.points)-1
                }
        )

    @staticmethod
    def get_geojson_feature_collection(paths: List['Path']):
        return FeatureCollection([p.get_geojson_feature() for p in paths])

    @staticmethod
    def export(collection: FeatureCollection, filename: str):
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        with open(filename, "w") as f:
            geojson.dump(collection, f)
        print("LineString export completed.\n")


class Segment:
    def __init__(self, id1, id2, coord1, coord2, rank):
        self.id1 = id1
        self.id2 = id2
        self.coord1 = coord1
        self.coord2 = coord2
        self.rank = rank
        self.weight = 0
        self.agony = 0

    def __str__(self):
        return "Segment from "+str(self.id1)+" to "+str(self.id2)+" taken "+str(self.weight)+ "times"

    def set_weight(self, w):
        self.weight = w

    def set_agony(self, a):
        self.agony = a

    def get_geojson_geometry(self):
        coordinates = []
        coordinates.append((self.coord1[0], self.coord1[1]))
        coordinates.append((self.coord2[0], self.coord2[1]))
        return LineString(coordinates)

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_geometry(),
            properties = {
                'endpoint1': self.id1,
                'endpoint2': self.id2,
                'rank': self.rank,
                'weight': self.weight
                }
        )

    @staticmethod
    def get_geojson_feature_collection(segments: List['Segment']):
        return FeatureCollection([p.get_geojson_feature() for s in segments])

    @staticmethod
    def export(collection: FeatureCollection, filename: str):
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        with open(filename, "w") as f:
            geojson.dump(collection, f)
        print("Segment export completed.\n")


#------------------------------- EXPORTS --------------------------------------

def create_point_map(gateways: List['GwPoint'], filename: str):
    print("Creating Gateway points collection. Number of points:", len(gateways))
    return GwPoint.get_geojson_feature_collection(gateways)

def create_paths_map(gateways: List['GwPoint'], filename: str):
    paths: List['Path'] = []
    (src, dest) = find_endpoints(gateways)
    idx = 0
    dest_index = 0
    while 'D' not in (src, dest): # Loop until the end of the file is reached
        for m in gateways[dest_index:]:
            dest_index += 1
            if m.zone == dest:
                break
        src_index = dest_index
        for m in reversed(gateways[:dest_index]):
            src_index -= 1
            if m.zone == src:
                break
        paths.append(Path(
            idx,
            src,
            dest,
            gateways[src_index:dest_index]
        ))
        idx += 1
        src_index = dest_index
        (src, dest) = find_endpoints(gateways[src_index:])
    print("Creating LineString path collection. Number of paths:", len(paths))
    return Path.get_geojson_feature_collection(paths)

def create_segment_map(segments: List['Segment'], filename: str):
    print("Creating Segment path collection. Number of segments:", len(segments))
    return Path.get_geojson_feature_collection(segments)
