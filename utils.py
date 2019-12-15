from common import *
import struct
import time
import dill
from functools import reduce
from itertools import groupby
import random
import numpy as np

def random_gen(k, points):
    random.shuffle(points)
    random_list = []
    for i in range(k):
        random_list.append(points[i])
    return random_list

def distanceSquared(vector1,vector2):
  # dist = 0
    vector1 = np.array(vector1)
    vector2 = np.array(vector2)
    dist = np.sum((vector1 - vector2)**2)
    return dist

def addPoints(x,y):
    point = np.array(x[0]) + np.array(y[0]).tolist()
    num = x[1] + y[1]
    return  (point, num)

def average(newpoint):
    point = []
    point = (np.array(newpoint[1][0])/newpoint[1][1]).tolist()
    return (newpoint[0],(point,newpoint[1][1]))

def closestPoint(point, centriods,K):
    vector1 = np.array(point)
    vector1 = np.stack([vector1 for _ in range(K)])
    vector2 = np.array(centriods)
    dist = np.sum((vector1 - vector2)**2, axis=1)
    bestIndex = np.argmin(dist)
    return bestIndex

def parse_host_names(host_names):
    """Transform host names string to list"""
    return host_names.split(',')


def dfs2local_path(dfs_path):
    """Convert DFS path to local path"""
    return data_node_dir + dfs_path


def serialize(obj):
    """Use pickle to serialize object for sending data"""
    return dill.dumps(obj)


def deserialize(pkl):
    """Use pickle to deserialize object for receiving data"""
    return dill.loads(pkl)


def key_func(x):
    return x[0]


def value_func(x):
    return x[1]


def reduce_by_key(data, func):
    # tuple-based record by default
    sorted_partition = sorted(data, key=key_func)
    key_group = [(key, list(map(value_func, group))) for key, group in groupby(sorted_partition, key=key_func)]
    # example of `key_group`: [ ('word1', [1, 1, 1]), ('word2', [1, 1]) ]
    result = [(key, reduce(func, group)) for key, group in key_group]
    return result


# Use a header to indicate data size, refer to https://stackoverflow.com/a/27429611
def send_msg(sock, data):
    # Pack the data size into an int with big-endian
    header = struct.pack('>i', len(data))
    sock.sendall(header)
    sock.sendall(data)


def recv_msg(sock):
    # Parse the header, which is a 4-byte int
    header = sock.recv(4)
    num_try = 5
    while len(header) == 0 and num_try:
        print('Fail to receive data. Trying again...')
        time.sleep(0.1)
        header = sock.recv(4)
        num_try -= 1
    data = bytearray()
    if len(header) != 0:
        data_size = struct.unpack('>i', header)[0]
        while len(data) < data_size:
            part = sock.recv(min(BUF_SIZE, data_size-len(data)))
            data.extend(part)
    return data


class LockContext:
    def __init__(self, memory):
        self.lock = memory[4]

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, type, value, traceback):
        self.lock.release()
