#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import time
import socket
import pickle
import traceback
import pickle
from multiprocessing import Process, Manager
from io import StringIO
import numpy as np
import pandas as pd
from utils import *
from common import *
import logging

class Operation:
    def __call__(self, *args, **kwargs):
        raise NotImplementedError('Subclasses of Operation must override __call__()!')

    @staticmethod
    def clear_memory():
        """Clear memory of every nodes after performing an action"""
        def handle(host_name):
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "clear_memory"
            # make sure the operation is done
            message = ''
            while message != '200':
                logger.debug('[clear_memory] connect ' + host_name)
                send_msg(worker_sock, serialize(request))
                message = deserialize(recv_msg(worker_sock))
            worker_sock.close()

        jobs = []
        for host_name in host_list:
            process = Process(target=handle, args=[host_name])
            process.start()
            jobs.append(process)

        for job in jobs:
            job.join()


class Transformation(Operation):
    def __init__(self, func):
        self.func = func

    def map(self, partition_tbl, step):
        for blk_no, host_name in partition_tbl.items():
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "map {} {}".format(blk_no, step)
            logger.debug('[map] connect ' + host_name)
            send_msg(worker_sock, serialize(request))
            time.sleep(0.1)
            send_msg(worker_sock, serialize(self.func))
            worker_sock.close()

    def filter_(self, partition_tbl, step):
        for blk_no, host_name in partition_tbl.items():
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "filter_ {} {}".format(blk_no, step)
            logger.debug('[filter_] connect ' + host_name)
            send_msg(worker_sock, serialize(request))
            time.sleep(0.1)
            send_msg(worker_sock, serialize(self.func))
            worker_sock.close()

class Action(Operation):
    def __init__(self):
        super(Action, self).__init__()
        
    def take(self, partition_tbl, num, step):
        result = []
        # take lines starting from bulk 0
        blk_no = 0
        logger.debug('partition table: %s' % partition_tbl)
        while num > 0 or num == -1:
            host_name = partition_tbl.get(blk_no)
            if host_name is None:
                break
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            request = "take {} {} {}".format(blk_no, num, step)
            logger.debug('[take] connect ' + host_name)
            # send_msg(worker_sock, serialize(request))
            send_msg(worker_sock, serialize(request))
            lines = deserialize(recv_msg(worker_sock))
            worker_sock.close()

            result.extend(lines)
            if num != -1:
                num -= len(lines)
            blk_no += 1

        # some nodes will have to clear memory before finishing all operations, but that's ok
        self.clear_memory()
        return result

class TextFileOp(Transformation):
    def __init__(self, file_path):
        self.file_path = file_path

    def __call__(self, step, *args, **kwargs):
        """Load file from DFS and return the partition table"""
        request = "get_fat_item {}".format(self.file_path)
        logger.debug("Request: {}".format(request))

        manager_sock = socket.socket()
        manager_sock.connect((name_node_host, name_node_port))

        # 1. get FAT from the Manager
        send_msg(manager_sock, serialize(request))
        fat_pd = recv_msg(manager_sock)
        fat_pd = deserialize(fat_pd)
        logger.debug("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 2. let workers to load files into memory
        # make sure the memory is empty at first
        self.clear_memory()
        partition_tbl = {}  # blk_no: host_name
        for idx, row in fat.iterrows():
            worker_sock = socket.socket()
            # randomly choose a host
            host_name = np.random.choice(parse_host_names(row['host_names']), size=1)[0]
            blk_no = row['blk_no']
            partition_tbl[blk_no] = host_name
            worker_sock.connect((host_name, data_node_port))

            request = "text_file {} {} {}".format(self.file_path, blk_no, step)
            send_msg(worker_sock, serialize(request))
            worker_sock.close()
        logger.debug('partition table: %s' % partition_tbl)
        return partition_tbl


class MapOp(Transformation):
    def __init__(self, func):
        super(MapOp, self).__init__(func)

    def __call__(self, partition_tbl, step, *args, **kwargs):
        self.map(partition_tbl, step)


class FilterOp(Transformation):
    def __init__(self, func):
        super(FilterOp, self).__init__(func)

    def __call__(self, partition_tbl, step, *args, **kwargs):
        self.filter_(partition_tbl, step)


class ReduceByKeyOp(Transformation):
    def __call__(self, partition_tbl, step, *args, **kwargs):
        """
        1. Perform local reducing in each machine;
        2. Let the nodes exchange data to perform global reducing
        3. Collect new partition table
        """
        def handle(host_name, partition_tbl, cur_blk_no, lock):
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            blk_nos = ','.join(blk_nos_on_each_host[host_name]) + ','
            request = "local_reduce_by_key {} {}".format(blk_nos, step)
            logger.debug('request:', request)
            logger.debug('[local_reduce_by_key] connect ' + host_name)
            send_msg(worker_sock, serialize(request))
            send_msg(worker_sock, serialize(self.func))
            worker_sock.close()

            # must set a new socket for another request
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            logger.debug('[transfer_reduced_data] connect ' + host_name)
            request = 'transfer_reduced_data'
            send_msg(worker_sock, serialize(request))
            while True:
                try:
                    received = recv_msg(worker_sock)
                    blk_nos = deserialize(received)
                    if not isinstance(blk_nos, list):
                        logger.debug('try to get bulk numbers of %s but receive:' % host_name, blk_nos)
                        continue
                    logger.debug('received blk numbers:', blk_nos)
                    break
                except Exception:
                    logger.debug('fail to receive sub bulk numbers:')
                    traceback.logger.debug_exc()
                    pass
            worker_sock.close()

            # rearrange bulk number
            worker_sock = socket.socket()
            worker_sock.connect((host_name, data_node_port))
            new_blk_nos = {}  # {old_blk_no: new_blk_no}
            for old_blk_no in blk_nos:
                lock.acquire()
                new_blk_nos[old_blk_no] = str(cur_blk_no.value)
                partition_tbl[cur_blk_no.value] = host_name
                cur_blk_no.value += 1
                lock.release()
            message = ''
            while message != '200':
                logger.debug('[update_blk_no] connect ' + host_name)
                request = 'update_blk_no'
                send_msg(worker_sock, serialize(request))
                send_msg(worker_sock, serialize(new_blk_nos))
                message = deserialize(recv_msg(worker_sock))
            worker_sock.close()

        manager = Manager()
        new_partition_tbl = manager.dict()
        cur_blk_no = manager.Value('i', 0)
        lock = manager.Lock()
        jobs = []
        blk_nos_on_each_host = {host:[] for host in host_list}
        for blk_no, host in partition_tbl.items():
            blk_no = str(blk_no)
            blk_nos_on_each_host[host].append(blk_no)
        for host_name in host_list:
            process = Process(target=handle, args=(host_name, new_partition_tbl, cur_blk_no, lock))
            process.start()
            jobs.append(process)

        for job in jobs:
            job.join()

        return dict(new_partition_tbl)


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
    try:
        partition_table = TextFileOp('/wc_dataset.txt')(0)
        logger.debug('[partition table]\n%s' % partition_table)

        MapOp(lambda x: (x, 1))(partition_table, 1)
        partition_table = ReduceByKeyOp(lambda a, b: a + b)(partition_table, 2)
        logger.debug('[new partition table]\n%s' % partition_table)
        FilterOp(lambda x: key_func(x) == 'American')(partition_table, 3)
        take_res = TakeOp(20)(partition_table, 4)
        take_res = [str(x) for x in take_res]
        logger.debug('[take]\n%s' % '\n'.join(take_res))
        # collect_res = CollectOp()(partition_table, 3)
        # collect_res = [str(x) for x in collect_res]
        # logger.debug('[collect]\n%s' % '\n'.join(collect_res))
    finally:
        # clear memory of all nodes whatever
        Operation.clear_memory()
