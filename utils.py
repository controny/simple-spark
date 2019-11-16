from common import *


def parse_host_names(host_names):
    """Transform host names string to list"""
    return host_names.split(',')


def recvall(sock):
    data = bytearray()
    while True:
        part = sock.recv(BUF_SIZE)
        data.extend(part)
        if len(part) < BUF_SIZE:
            # either 0 or end of data
            break
    return data
