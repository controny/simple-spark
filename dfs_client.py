# coding=utf-8
import os
import numpy as np
import socket
import time
from io import StringIO

import pandas as pd
from collections import defaultdict
import math
from tqdm import tqdm

from common import *
from utils import *


class DFSClient:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def ls(self, dfs_path):
        # 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        try:
            cmd = "ls {}".format(dfs_path)
            send_msg(self.name_node_sock, bytes(cmd, encoding='utf-8'))
            response_msg = recv_msg(self.name_node_sock)
            print(str(response_msg, encoding='utf-8'))
        except Exception as e:
            print(e)
        finally:
            pass
    
    def copyFromLocal(self, local_path, dfs_path):
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))

        # In case that the dfs_path is a directory path
        if os.path.basename(dfs_path) == '':
            dfs_path = os.path.join(dfs_path, os.path.basename(local_path))

        # Split the file by line and get size for each bulk in advance
        bulk_sizes = self.split_file_by_line(local_path)
        num_blk = len(bulk_sizes)
        request = "new_fat_item {} {}".format(dfs_path, num_blk)
        print("Request: {}".format(request))

        # 从NameNode获取一张FAT表
        send_msg(self.name_node_sock, bytes(request, encoding='utf-8'))
        fat_pd = recv_msg(self.name_node_sock)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个向目标DataNode发送数据块
        print('Sending data to Data Nodes')
        host_load = defaultdict(lambda: 0)
        # Open in binary mode to avoid a strange bug about file position
        fp = open(local_path, 'rb')
        for idx, row in tqdm(fat.iterrows()):
            data = fp.read(bulk_sizes[idx])
            
            for host_name in parse_host_names(row['host_names']):
                data_node_sock = socket.socket()
                data_node_sock.connect((host_name, data_node_port))
                blk_path = dfs_path + ".blk{}".format(row['blk_no'])

                request = "store {}".format(blk_path)
                send_msg(data_node_sock, bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                # send_msg(data_node_sock, bytes(data, encoding='utf-8'))
                send_msg(data_node_sock, data)
                data_node_sock.close()
                host_load[host_name] += 1
        fp.close()
        print('Host load:\n' + '\n'.join(['%s: %d' % (k, v) for k, v in host_load.items()]))

    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))

        # In case that the local_path is a directory path
        if os.path.basename(local_path) == '':
            local_path = os.path.join(local_path, os.path.basename(dfs_path))

        # 从NameNode获取一张FAT表
        send_msg(self.name_node_sock, bytes(request, encoding='utf-8'))
        fat_pd = recv_msg(self.name_node_sock)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        fp = open(local_path, "w")
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            # randomly choose a host
            host_name = np.random.choice(parse_host_names(row['host_names']), size=1)[0]
            data_node_sock.connect((host_name, data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "load {}".format(blk_path)
            send_msg(data_node_sock, bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            data = recv_msg(data_node_sock)
            data = str(data, encoding='utf-8')
            fp.write(data)
            data_node_sock.close()
        fp.close()
    
    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        
        # 从NameNode获取改文件的FAT表，获取后删除
        send_msg(self.name_node_sock, bytes(request, encoding='utf-8'))
        fat_pd = recv_msg(self.name_node_sock)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个告诉目标DataNode删除对应数据块
        for idx, row in fat.iterrows():
            for host_name in parse_host_names(row['host_names']):
                data_node_sock = socket.socket()
                data_node_sock.connect((host_name, data_node_port))
                blk_path = dfs_path + ".blk{}".format(row['blk_no'])

                request = "rm {}".format(blk_path)
                send_msg(data_node_sock, bytes(request, encoding='utf-8'))
                response_msg = recv_msg(data_node_sock)
                print(str(response_msg, encoding='utf-8'))

                data_node_sock.close()
    
    def format(self):
        request = "format"
        print(request)
        
        send_msg(self.name_node_sock, bytes(request, encoding='utf-8'))
        print(str(recv_msg(self.name_node_sock), encoding='utf-8'))
        
        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            
            send_msg(data_node_sock, bytes("format", encoding='utf-8'))
            print(str(recv_msg(data_node_sock), encoding='utf-8'))
            
            data_node_sock.close()

    def mapReduce(self, dfs_path):
        start_time = time.time()
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))

        send_msg(self.name_node_sock, bytes(request, encoding='utf-8'))
        fat_pd = recv_msg(self.name_node_sock)
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat_pd = pd.read_csv(StringIO(fat_pd))

        # Get assignment table
        assign_table = self.map_reduce_assign(fat_pd)

        local_sums = []
        local_sum_squares = []
        local_counts = []
        data_node_socks = []
        print('Mapping data to Data Nodes')
        for row in tqdm(assign_table):
            # Let each data node perform reducing operation
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))

            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            request = "reduce {}".format(blk_path)
            send_msg(data_node_sock, bytes(request, encoding='utf-8'))
            data_node_socks.append(data_node_sock)

        # Try to receive message asynchronously
        while len(data_node_socks) != 0:
            for data_node_sock in data_node_socks[:]:
                response_msg = recv_msg(data_node_sock)
                if len(response_msg) == 0:
                    # If not receive any data, continue and try later
                    continue
                # Parse the local results
                local_result = pd.read_csv(StringIO(str(response_msg, encoding='utf-8'))).iloc[0]
                local_sums.append(local_result['sum'])
                local_sum_squares.append(local_result['sum_square'])
                local_counts.append(local_result['count'])

                data_node_sock.close()
                # The socket is finished, so we can remove it
                data_node_socks.remove(data_node_sock)

        local_sums = np.asarray(local_sums)
        local_sum_squares = np.asarray(local_sum_squares)
        local_counts = np.asarray(local_counts)
        total_count = np.sum(local_counts)
        mean = np.sum(local_sums) / total_count
        variance = np.sum(local_sum_squares) / total_count - np.square(mean)
        end_time = time.time()
        print('Mean: %f, Var: %f, Time: %f s' % (mean, variance, end_time-start_time))

    def mapReduceTest(self, local_path):
        """Compute mean and var locally to test the validity of MapReduce"""
        start_time = time.time()
        numbers = []
        with open(local_path) as f:
            for line in f.readlines():
                # Skip the first column and parse the others as float
                numbers.extend([float(x) for x in line.strip().split(' ')[1:]])
        numbers = np.asarray(numbers)
        mean = float(np.mean(numbers))
        variance = float(np.var(numbers))
        end_time = time.time()
        print('Mean: %f, Var: %f, Time: %f s' % (mean, variance, end_time-start_time))

    @staticmethod
    def split_file_by_line(local_path):
        """Split file by line and return a list of bulk sizes"""
        bulk_sizes = []
        # Open in binary mode to avoid a strange bug about file position
        with open(local_path, 'rb') as f:
            eof = False
            while not eof:
                begin_pos = f.tell()
                last_pos = 0
                while f.tell() - begin_pos <= dfs_blk_size:
                    last_pos = f.tell()
                    if f.readline() == b'':
                        eof = True
                        break
                if not eof:
                    # Return to the last position
                    f.seek(last_pos)
                bulk_size = f.tell() - begin_pos
                bulk_sizes.append(bulk_size)

            # Check all bulk sizes sum up to the original file size
            assert sum(bulk_sizes) == os.stat(f.fileno()).st_size

        return bulk_sizes


    @staticmethod
    def map_reduce_assign(fat_pd):
        """Return assignment table"""
        assign_table = []
        host_load = defaultdict(lambda: 0)
        for idx, row in fat_pd.iterrows():
            host = np.random.choice(parse_host_names(row['host_names']), size=1)[0]
            host_load[host] += 1
            assign_table.append({
                'blk_no': row['blk_no'],
                'host_name': host
            })

        print('Host load: \n' + '\n'.join(['%s: %d' % (k, v) for k, v in host_load.items()]))

        return assign_table


# 解析命令行参数并执行对于的命令
import sys

argv = sys.argv
argc = len(argv) - 1

client = DFSClient()

cmd = argv[1]
if cmd == '-ls':
    if argc == 2:
        dfs_path = argv[2]
        client.ls(dfs_path)
    else:
        print("Usage: python client.py -ls <dfs_path>")
elif cmd == "-rm":
    if argc == 2:
        dfs_path = argv[2]
        client.rm(dfs_path)
    else:
        print("Usage: python client.py -rm <dfs_path>")
elif cmd == "-copyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.copyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
elif cmd == "-copyToLocal":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.copyToLocal(dfs_path, local_path)
    else:
        print("Usage: python client.py -copyFromLocal <dfs_path> <local_path>")
elif cmd == "-format":
    client.format()
elif cmd == "-mapReduce":
    if argc == 2:
        dfs_path = argv[2]
        client.mapReduce(dfs_path)
    else:
        print("Usage: python client.py -mapReduce <dfs_path>")
elif cmd == "-mapReduceTest":
    if argc == 2:
        local_path = argv[2]
        client.mapReduceTest(local_path)
    else:
        print("Usage: python client.py -mapReduce <local_path>")
else:
    print("Undefined command: {}".format(cmd))
