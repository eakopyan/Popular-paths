import csv
from typing import List, Dict
from geoProcessor import GwPoint, find_endpoints


#---------------------------- OBJECT DEFINITION -------------------------------

class Gateway:
    def __init__(self, gw_ID, gw_LAC, gw_NIDT, gw_azimuth, gw_x, gw_y): # ID = CI
        self.id = gw_ID
        self.lac = gw_LAC
        self.nidt = gw_NIDT
        self.azimuth = gw_azimuth
        self.longitude, self.latitude = float(gw_x), float(gw_y)

    def print_gateway(self):
        print('ID:', self.id, ' \tLAC:', self.lac, '\tCoordinates:', self.longitude, self.latitude)

    def find_um_match(self, measures: List['UserMeasure']):
        for measure in measures:
            if (self.id, self.lac) == (measure.id, measure.lac):
                return measure
            else:
                pass

    def get_zone(self, user_file: str):
        path = get_path(user_file)
        measure = self.find_um_match(path)
        return measure.zone

    def get_timestamp(self, user_file: str):
        path = get_path(user_file)
        measure = self.find_um_match(path)
        return measure.timestamp


class UserMeasure:
    def __init__(self, timestamp, lac, id, zone):
        self.timestamp = timestamp
        self.lac = lac
        self.id = id
        self.zone = zone[0]

    def print_userMeasure(self):
        print('Date:', self.timestamp, '\tLAC:', self.lac, '\tID:', self.id, '\tZone:', self.zone)

    def find_gw_match(self):
        gateways = get_all_gateways()
        for gw in gateways:
            if (gw.id, gw.lac) == (self.id, self.lac):
                return gw
            else:
                pass


class DAG:
    def __init__(self, list: List['UserMeasure']):
        self.list = list
        self.length = len(list)

    def print_DAG(self):
        for m in self.list:
            m.print_userMeasure()

    def remove_loop(self, id):
        measures = self.list
        first, last = 0,0
        for i, m in enumerate(measures):
            if (m.id, m.lac) == id:
                first = i+1
                break
        for j, m in reversed(list(enumerate(measures))):
            if (m.id, m.lac) == id:
                last = j+1
                break
        self.list = measures[:first] + measures[last:]


#------------------------------ FUNCTIONS -------------------------------------

def get_all_gateways():
    """Function to extract all gateways from data file. Takes data from file
    "cells_abcd.csv".

    Returns
    -------
    gateways : List[Gateway]
        List of all gateways with all information
    """

    gateways: List[Gateway] = []
    with open("/home/agora/Documents/Popular_paths/Data/saint_foy/saint_foy/cells_abcd.csv", "r") as f:
        csv_reader = csv.reader(f, delimiter = ';')
        for line in csv_reader:
            if line[0] == 'ci': # Title line
                pass
            else:
                gateways.append(Gateway(
                    line[0], # ID
                    line[1], # LAC
                    line[2], # NIDT
                    line[3], # Azimuth
                    line[5], # Longitude
                    line[6]  # Latitude
                ))
    return gateways


def get_path(user_file:str):
    """Function to extract data from user file, point by point.

    Parameters
    ----------
    user_file : str
        Name of file to parse

    Returns
    -------
    path : List[UserMeasure]
        List of data as paths
    """

    path: List[UserMeasure] = []
    with open(user_file) as f:
        for line in f.readlines():
            data = line.split(",")
            path.append(UserMeasure(
                data[0], # Timestamp
                data[1], # LAC
                data[2], # ID
                data[3], # Zone
            ))
    return path


def extract_paths(measures: List['UserMeasure']) -> List['GwPoint']:
    """Function to extract path from user data, point by point. Performs
    noise reduction between endpoints (A and B) by a DAG treatment (see
    dataProcessor.to_DAG for more information).

    Parameters
    ----------
    measures : List[UserMeasure]
        List of measures to process

    Returns
    -------
    path : List[GwPoint]
        List of points in path, cleaned of noise
    """

    path: List['GwPoint'] = []
    measures = sorted(measures, key = lambda k: k.timestamp)
    (src, dest) = find_endpoints(measures)
    dest_index = 0
    while 'D' not in (src, dest): # Loop until the end of the file is reached
        for m in measures[dest_index:]:
            dest_index += 1
            if m.zone == dest:
                break
        src_index = dest_index
        for m in reversed(measures[:dest_index]):
            src_index -= 1
            if m.zone == src:
                break
        dag = to_DAG(measures[src_index:dest_index])
        for d in dag.list:
            path.append(GwPoint(
                d.id,
                d.lac,
                d.find_gw_match().azimuth,
                d.find_gw_match().longitude,
                d.find_gw_match().latitude,
                d.zone,
                d.timestamp
            ))
        src_index = dest_index
        (src, dest) = find_endpoints(measures[src_index:])
    return path


def to_DAG(measures: List['UserMeasure']):
    dag = DAG(measures)
    loop_gw = {} # key=(id,lac), values=weight of a potential loop point
    for m in dag.list:
        if (m.id, m.lac) not in loop_gw: # There is a cycle
            loop_gw[(m.id, m.lac)] = 1
        else:
            loop_gw[(m.id, m.lac)] += 1
    for id, nb in loop_gw.items():
        if nb > 1:
            dag.remove_loop(id)
    return dag
