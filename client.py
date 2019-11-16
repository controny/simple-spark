# coding=utf-8
import os
import numpy as np
import socket
import time
from io import StringIO

import pandas as pd

from common import *
from utils import *


class Client:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def ls(self, dfs_path):
        # 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        try:
            cmd = "ls {}".format(dfs_path)
            self.name_node_sock.send(bytes(cmd, encoding='utf-8'))
            response_msg = self.name_node_sock.recv(BUF_SIZE)
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

        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个向目标DataNode发送数据块
        fp = open(local_path)
        for idx, row in fat.iterrows():
            data = fp.read(int(row['blk_size']))
            
            for host_name in parse_host_names(row['host_names']):
                data_node_sock = socket.socket()
                print('connecting', host_name)
                data_node_sock.connect((host_name, data_node_port))
                blk_path = dfs_path + ".blk{}".format(row['blk_no'])

                request = "store {}".format(blk_path)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                data_node_sock.send(bytes(data, encoding='utf-8'))
                data_node_sock.close()
        fp.close()
    
    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
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
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            data = data_node_sock.recv(BUF_SIZE)
            data = str(data, encoding='utf-8')
            fp.write(data)
            data_node_sock.close()
        fp.close()
    
    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        
        # 从NameNode获取改文件的FAT表，获取后删除
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
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
                data_node_sock.send(bytes(request, encoding='utf-8'))
                response_msg = data_node_sock.recv(BUF_SIZE)
                print(response_msg)

                data_node_sock.close()
    
    def format(self):
        request = "format"
        print(request)
        
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        
        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            
            data_node_sock.send(bytes("format", encoding='utf-8'))
            print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))
            
            data_node_sock.close()

    def mapReduce(self, dfs_path):
        # TODO Split by line instead of original bulk
        request = "map_reduce_assign {}".format(dfs_path)
        print("Request: {}".format(request))

        # Get assignment table from Name Node
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        assign_pd = self.name_node_sock.recv(BUF_SIZE)
        assign_pd = str(assign_pd, encoding='utf-8')
        print("Assign: \n{}".format(assign_pd))
        assign_table = pd.read_csv(StringIO(assign_pd))

        local_sums = []
        local_sum_squares = []
        local_counts = []
        for idx, row in assign_table.iterrows():
            # Let each data node perform reducing operation
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))

            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            request = "reduce {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            response_msg = data_node_sock.recv(BUF_SIZE)
            # Parse the local results
            local_result = pd.read_csv(StringIO(str(response_msg, encoding='utf-8'))).iloc[0]
            local_sums.append(local_result['local_sum'])
            local_sum_squares.append(local_result['local_sum_square'])
            local_counts.append(local_result['local_count'])

            data_node_sock.close()

        local_sums = np.asarray(local_sums)
        local_sum_squares = np.asarray(local_sum_squares)
        local_counts = np.asarray(local_counts)
        total_count = np.sum(local_counts)
        mean = np.sum(local_sums) / total_count
        std = np.sum(local_sum_squares) / total_count - np.square(mean)
        print('Mean: %f, Std: %f' % (mean, std))


# 解析命令行参数并执行对于的命令
import sys

argv = sys.argv
argc = len(argv) - 1

client = Client()

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
        client.rm(dfs_path)
    else:
        print("Usage: python client.py -mapReduce <dfs_path>")
else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format | -mapReduce> other_arguments")
