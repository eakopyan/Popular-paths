import errno
import os
import geojson
from geojson import Point, LineString, Feature, FeatureCollection
from typing import List

class BSPoint:
    def __init__(self, id, lon, lat, zone, rank, dist):
        self.id = id
        self.lon = lon
        self.lat = lat
        self.zone = zone
        self.rank = rank[0]
        self.dist = dist


    def __str__(self):
        return "BaseStation ID: "+str(self.id)+"\tCoordinates: "+str(self.lon)+" ; "+str(self.lat)

    def get_geojson_point(self):
        return Point((self.lat, self.lon)) # Keep reversed for folium

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_point(),
            properties = {
                'ID': self.id[0],
                'LAC': self.id[1],
                'Zone': self.zone,
                'Rank': self.rank,
                'Distance': self.dist
            }
        )

    @staticmethod
    def get_geojson_feature_collection(points: List['BSPoint']):
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
        print("BSPoint export completed.")


class ArcLine:
    def __init__(self, id_first, id_last, coord_first, coord_last, weight, agony, duration):
        self.id_first = id_first
        self.id_last = id_last
        self.coord_first = coord_first
        self.coord_last = coord_last
        self.weight = weight
        self.agony = agony
        self.duration = duration

    def __str__(self):
        return "Arc between BS "+str(self.id_first)+" and "+str(self.id_last)+"\tWeight: "+str(self.weight)

    def get_geojson_line(self):
        coordinates = []
        coordinates.append((self.coord_first[0], self.coord_first[1]))
        coordinates.append((self.coord_last[0], self.coord_last[1]))
        return LineString(coordinates)

    def get_geojson_feature(self):
        return Feature(
            geometry = self.get_geojson_line(),
            properties = {
                'First ID': self.id_first[0],
                'First LAC': self.id_first[1],
                'Last ID': self.id_last[0],
                'Last LAC': self.id_last[1],
                'Weight': self.weight,
                'Agony': self.agony,
                'Travel time': self.duration
            }
        )

    @staticmethod
    def get_geojson_feature_collection(lines: List['ArcLine']):
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
        print("ArcLine export completed.")


#------------------------------------------------------------------------------

def to_BSPoint(base_stations: List['BaseStation']):
    bspoints: List['BSPoint'] = []
    for bs in base_stations:
        bspoints.append(BSPoint(
            bs.id,
            bs.lon,
            bs.lat,
            bs.zone,
            bs.rank,
            bs.dist
        ))
    return bspoints


def to_ArcLine(arcs: List['Arc']):
    arclines: List['ArcLine'] = []
    for a in arcs:
        arclines.append(ArcLine(
            a.first,
            a.last,
            a.coord_first,
            a.coord_last,
            a.weight,
            a.agony,
            a.get_average_duration()
        ))
    return arclines


def create_geojson_points(base_stations: List['BaseStation']):
    print('\nCreating BSPoint collection. Number of points:', len(base_stations))
    bspoints = to_BSPoint(base_stations)
    return BSPoint.get_geojson_feature_collection(bspoints)

def create_geojson_lines(arcs: List['ArcLine']):
    print('\nCreating ArcLine collection. Number of lines:', len(arcs))
    arclines = to_ArcLine(arcs)
    return ArcLine.get_geojson_feature_collection(arclines)
