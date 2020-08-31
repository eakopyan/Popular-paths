import errno
import os
import geojson
from geojson import Point, LineString, Feature, FeatureCollection
from typing import List


class PlacePoint:
    def __init__(self, id, lon, lat, zone, cluster_id):
        self.id = id
        self.lon = lon
        self.lat = lat
        self.zone = zone
        self.cluster_id = cluster_id

    def __str__(self):
        return "Place ID: "+str(self.id)+"\tZone: "+self.zone+"\tCluster: "+str(self.cluster_id)

    def get_geojson_point(self):
        return Point((self.lat, self.lon)) # Keep reversed for folium

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_point(),
            properties = {
                'Place ID': self.id,
                'Zone': self.zone,
                'Cluster ID': self.cluster_id
            }
        )

    @staticmethod
    def get_geojson_feature_collection(points: List['PlacePoint']):
        return FeatureCollection([p.get_geojson_feature() for p in points])

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
        print("PlacePoint export completed.")


class ClusterPoint:
    def __init__(self, id, lon, lat, weight):
        self.id = id
        self.lon = lon
        self.lat = lat
        self.weight = weight

    def __str__(self):
        return "Cluster ID: "+self.id+"\tCoordinates: "+str(self.lon)+", "+str(self.lat)

    def get_geojson_point(self):
        return Point((self.lat, self.lon)) # Keep reversed for folium

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_point(),
            properties = {
                'Cluster ID': self.id,
                'Weight': self.weight
            }
        )

    @staticmethod
    def get_geojson_feature_collection(points: List['ClusterPoint']):
        return FeatureCollection([p.get_geojson_feature() for p in points])

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
        print("ClusterPoint export completed.")


class ClusterLine:
    def __init__(self, id_first, id_last, coord_first, coord_last, mag):
        self.id_first = id_first
        self.id_last = id_last
        self.coord_first = coord_first
        self.coord_last = coord_last
        self.magnitude = mag

    def __str__(self):
        return "ClusterLine from "+self.id_first+" to "+self.id_last+"\tMagnitude: "+str(self.magnitude)

    def get_geojson_line(self):
        coordinates = []
        coordinates.append((float(self.coord_first[0]), float(self.coord_first[1])))
        coordinates.append((float(self.coord_last[0]), float(self.coord_last[1])))
        return LineString(coordinates)

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_line(),
            properties = {
                'Source ID': self.id_first,
                'Destination ID': self.id_last,
                'Magnitude': self.magnitude
            }
        )

    @staticmethod
    def get_geojson_feature_collection(lines: List['ClusterLine']):
        return FeatureCollection([l.get_geojson_feature() for l in lines])

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
        print("ClusterLine export completed.")

#------------------------------------------------------------------------------
def to_PlacePoint(points: List['Place']):
    ppoints: List['PlacePoint'] = []
    for p in points:
        ppoints.append(PlacePoint(
            str(p.id),
            p.lon,
            p.lat,
            p.zone,
            str(p.cluster_id)
        ))
    return ppoints

def to_ClusterPoint(clusters: List['ClusterPlace']):
    cpoints: List['ClusterPoint'] = []
    for c in clusters:
        cpoints.append(ClusterPoint(
            c.id,
            c.lon,
            c.lat,
            c.weight
        ))
    return cpoints

def to_ClusterLine(clusters: List['ClusterFlow']):
    clines: List['ClusterLine'] = []
    for c in clusters:
        clines.append(ClusterLine(
            c.id_first,
            c.id_last,
            c.coord_first,
            c.coord_last,
            c.magnitude
        ))
    return clines

def create_geojson_points(points: List['Place']):
    print('\nCreating PlacePoint collection. Number of points:', len(points))
    ppoints = to_PlacePoint(points)
    return PlacePoint.get_geojson_feature_collection(ppoints)

def create_geojson_clusters(clusters: List['ClusterPlace']):
    print('\nCreating ClusterPoint collection. Number of clusters:', len(clusters))
    points = to_ClusterPoint(clusters)
    return ClusterPoint.get_geojson_feature_collection(points)

def create_geojson_lines(clusters: List['ClusterFlow']):
    print('\nCreating ClusterLine collection. Number of flows:', len(clusters))
    lines = to_ClusterLine(clusters)
    return ClusterLine.get_geojson_feature_collection(lines)
