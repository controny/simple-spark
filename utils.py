from common import *
import struct


def parse_host_names(host_names):
    """Transform host names string to list"""
    return host_names.split(',')


# Use a header to indicate data size, refer to https://stackoverflow.com/a/27429611
def send_msg(sock, data):
    # Pack the data size into an int with big-endian
    header = struct.pack('>i', len(data))
    sock.sendall(header)
    sock.sendall(data)


def recv_msg(sock):
    # Parse the header, which is a 4-byte int
    data_size = struct.unpack('>i', sock.recv(4))[0]
    data = bytearray()
    while len(data) < data_size:
        part = sock.recv(BUF_SIZE)
        data.extend(part)
    return data
