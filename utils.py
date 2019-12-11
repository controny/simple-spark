from common import *
import struct
import time
import dill
from functools import reduce
from itertools import groupby


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
    return list(x.keys())[0]


def value_func(x):
    return list(x.values())[0]


def reduce_by_key(data, func):
    sorted_partition = sorted(data, key=key_func)
    grouped_partition = groupby(sorted_partition, key=key_func)
    return [{key: reduce(func, map(value_func, group))} for key, group in grouped_partition]


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
