#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import time
import socket
import pickle
from io import StringIO
import numpy as np
import pandas as pd
from utils import *
from common import *


class Operation:
    def __call__(self, *args, **kwargs):
        raise NotImplementedError('Subclasses of Operation must override __call__()!')


class Transformation(Operation):
    def __init__(self, func):
        self.func = func

    def map(self, partition_tbl, step):
        for blk_no, host_name in partition_tbl.items():
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "map {} {}".format(blk_no, step)
            print('[map] connect ' + host_name)
<<<<<<< HEAD
            send_msg(worker_sock, bytes(request, encoding='utf-8'))
            time.sleep(0.1)
            send_msg(worker_sock, serialize(self.func))
            worker_sock.close()
    def filter_(self, partition_tbl, step):
        for blk_no, host_name in partition_tbl.items():
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "filter_ {} {}".format(blk_no, step)
            print('[filter_] connect ' + host_name)
=======
>>>>>>> 6dd093dff06e07d687286d8971ba2cfd3833c4b9
            send_msg(worker_sock, bytes(request, encoding='utf-8'))
            time.sleep(0.1)
            send_msg(worker_sock, serialize(self.func))
            worker_sock.close()

class Action(Operation):
    def __init__(self):
        super(Action, self).__init__()
        
    def take(self, partition_tbl, num, step):
        result = []
        # take lines from bulk 0
        blk_no = 0
        while num > 0 or num == -1:
            host_name = partition_tbl.get(blk_no)
            if host_name is None:
                break
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "take {} {} {}".format(blk_no, num, step)
            print('[take] connect ' + host_name)
            send_msg(worker_sock, bytes(request, encoding='utf-8'))
            lines = deserialize(recv_msg(worker_sock))
            worker_sock.close()

            result.extend(lines)
            if num != -1:
                num -= len(lines)
            blk_no += 1

        self.clear_memory(partition_tbl)
        return result

    def clear_memory(self, partition_tbl):
        """Clear memory of every nodes after performing an action"""
        for blk_no, host_name in partition_tbl.items():
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "clear_memory"
            print('[clear_memory] connect ' + host_name)
            send_msg(worker_sock, bytes(request, encoding='utf-8'))
            worker_sock.close()

class TextFileOp(Transformation):
    def __init__(self, file_path):
        self.file_path = file_path

    def __call__(self, step, *args, **kwargs):
        """Load file from DFS and return the partition table"""
        request = "get_fat_item {}".format(self.file_path)
        print("Request: {}".format(request))

        manager_sock = socket.socket()
        manager_sock.connect((name_node_host, name_node_port))

        # 1. get FAT from the Manager
        send_msg(manager_sock, bytes(request, encoding='utf-8'))
        fat_pd = recv_msg(manager_sock)
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 2. let workers to load files into memory
        partition_tbl = {}  # blk_no: host_name
        for idx, row in fat.iterrows():
            worker_sock = socket.socket()
            # randomly choose a host
            host_name = np.random.choice(parse_host_names(row['host_names']), size=1)[0]
            blk_no = row['blk_no']
            partition_tbl[blk_no] = host_name
            worker_sock.connect((host_name, data_node_port))

            request = "text_file {} {} {}".format(self.file_path, blk_no, step)
            send_msg(worker_sock, bytes(request, encoding='utf-8'))
            worker_sock.close()
        print('partition table: %s' % partition_tbl)
        return partition_tbl


class MapOp(Transformation):
    def __init__(self, func):
        super(MapOp, self).__init__(func)

    def __call__(self, partition_tbl, step, *args, **kwargs):
        self.map(partition_tbl, step)
<<<<<<< HEAD

class FilterOp(Transformation):
    def __init__(self, func):
        super(FilterOp, self).__init__(func)

    def __call__(self, partition_tbl, step, *args, **kwargs):
        self.filter_(partition_tbl, step)
=======
>>>>>>> 6dd093dff06e07d687286d8971ba2cfd3833c4b9

class TakeOp(Action):
    def __init__(self, num):
        super(TakeOp, self).__init__()
        self.num = num
        
    def __call__(self, partition_tbl, step, *args, **kwargs):
        return self.take(partition_tbl, self.num, step)


class CollectOp(Action):
    def __call__(self, partition_tbl, step, *args, **kwargs):
        return self.take(partition_tbl, -1, step)


# Test
if __name__ == '__main__':
    partition_table = TextFileOp('/wc_dataset.txt')(0)
    print('[partition table]\n%s' % partition_table)

    MapOp(lambda x: {x: 1})(partition_table, 1)
    #
    take_res = TakeOp(20)(partition_table, 2)
    take_res = [str(x) for x in take_res]
    print('[take]\n%s' % '\n'.join(take_res))
    # collect_res = CollectOp()(partition_table)
    # print('[collect]\n%s' % '\n'.join(collect_res))
