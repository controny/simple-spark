# coding=utf-8
import os
import socket
import pandas as pd
import numpy as np
import traceback
import pickle

from common import *
from utils import *


class DataNode:
    def __init__(self):
        self.memory = {}

    def run(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(5)
            print('data node listening at port %s' % data_node_port)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))
                
                try:
                    # 获取请求方发送的指令
                    request = str(recv_msg(sock_fd), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    print(request)
                    
                    cmd = request[0]  # 指令第一个为指令类型

                    try:
                        if cmd == 'store':
                            # this command is kind of special
                            response = self.store(sock_fd, request[1])
                        else:
                            response = getattr(self, cmd)(*request[1:])
                    except Exception as e:
                        response = e

                    print('response:', response)
                    if type(response) is not bytes:
                        response = bytes(response, encoding='utf-8')
                    send_msg(sock_fd, response)
                except KeyboardInterrupt:
                    break
                except Exception:
                    traceback.print_exc()
                finally:
                    sock_fd.close()
        except KeyboardInterrupt:
            listen_fd.close()
        except Exception:
            traceback.print_exc()
        finally:
            listen_fd.close()
    
    def load(self, dfs_path):
        # 本地路径
        local_path = dfs2local_path(dfs_path)
        # 读取本地数据
        with open(local_path) as f:
            chunk_data = f.read(dfs_blk_size)
        
        return chunk_data
    
    def store(self, sock_fd, dfs_path):
        # 从Client获取块数据
        chunk_data = recv_msg(sock_fd)
        print('receive data with size = %d' % len(chunk_data))
        # 本地路径
        local_path = dfs2local_path(dfs_path)
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 将数据块写入本地文件
        with open(local_path, "wb") as f:
            f.write(chunk_data)
        
        return "Store chunk {} successfully~".format(local_path)
    
    def rm(self, dfs_path):
        local_path = dfs2local_path(dfs_path)
        rm_command = "rm -rf " + local_path
        os.system(rm_command)
        
        return "Remove chunk {} successfully~".format(local_path)
    
    def format(self):
        format_command = "rm -rf {}/*".format(data_node_dir)
        os.system(format_command)
        
        return "Format datanode successfully~"

    def text_file(self, dfs_base_path, blk_no):
        """Load file into memory"""
        dfs_path = '%s.blk%s' % (dfs_base_path, blk_no)
        local_path = dfs2local_path(dfs_path)
        with open(local_path) as f:
            chunk_data = f.read(dfs_blk_size)
            self.memory[blk_no] = chunk_data
        return "Load text file successfully~"

    def take(self, blk_no, num):
        """Take lines from chunk data in memory"""
        lines = self.memory[blk_no].split('\n')
        num = int(num)
        if num != -1:
            # -1 means take all data
            lines = lines[:num]
        # clear memory after performing an action
        self.clear_memory()
        return pickle.dumps(lines)

    def reduce(self, dfs_path):
        """Compute Sum(X), Sum(X^2) and Count(X) locally"""
        local_path = dfs2local_path(dfs_path)
        numbers = []
        with open(local_path) as f:
            for line in f.readlines():
                # Skip the first column and parse the others as float
                numbers.extend([float(x) for x in line.strip().split(' ')[1:]])
        numbers = np.asarray(numbers)
        local_count = len(numbers)
        local_sum = np.sum(numbers)
        local_sum_square = np.sum(np.square(numbers))
        data_pd = pd.DataFrame(
            data={
                'sum': local_sum,
                'sum_square': local_sum_square,
                'count': local_count
            }, index=[0])

        return data_pd.to_csv(index=False)

    def ping(self):
        return '200'

    def clear_memory(self):
        self.memory.clear()

# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
