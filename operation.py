#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import socket
import pickle
from io import StringIO
import numpy as np
import pandas as pd
from utils import *
from common import *

class Operation:
    def __call__(self, *args, **kwargs):
        raise NotImplementedError('subclasses must override __call__()!')

class Transformation(Operation):
    pass

class Action(Operation):
    def take(self, partition_tbl, num):
        result = []
        # take lines from bulk 0
        blk_no = 0
        while num > 0 or num == -1:
            host_name = partition_tbl.get(blk_no)
            if host_name is None:
                break
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "take {} {}".format(blk_no, num)
            send_msg(worker_sock, bytes(request, encoding='utf-8'))
            lines = pickle.loads(recv_msg(worker_sock))
            worker_sock.close()

            result.extend(lines)
            if num != -1:
                num -= len(lines)
            blk_no += 1
        return result

class TextFileOp(Transformation):
    def __init__(self, file_path):
        self.file_path = file_path

    def __call__(self, *args, **kwargs):
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

            request = "text_file {} {}".format(self.file_path, blk_no)
            send_msg(worker_sock, bytes(request, encoding='utf-8'))
            worker_sock.close()

        return partition_tbl

class TakeOp(Action):
    def __call__(self, partition_tbl, num, *args, **kwargs):
        return self.take(partition_tbl, num)

class CollectOp(Action):
    def __call__(self, partition_tbl, *args, **kwargs):
        return self.take(partition_tbl, -1)

# Test
if __name__ == '__main__':
    partition_table = TextFileOp('/wc_dataset.txt')()
    print('[partition table]\n%s' % partition_table)
    take_res = TakeOp()(partition_table, 20)
    print('[take]\n%s' % '\n'.join(take_res))
    collect_res = CollectOp()(partition_table)
    print('[collect]\n%s' % '\n'.join(collect_res))