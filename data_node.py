# coding=utf-8
import os
import socket
import pandas as pd
import numpy as np

from common import *
from utils import *


# DataNode支持的指令有:
# 1. load 加载数据块
# 2. store 保存数据块
# 3. rm 删除数据块
# 4. format 删除所有数据块

class DataNode:
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
                    request = str(recvall(sock_fd), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    print(request)
                    
                    cmd = request[0]  # 指令第一个为指令类型
                    
                    if cmd == "load":  # 加载数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.load(dfs_path)
                    elif cmd == "store":  # 存储数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.store(sock_fd, dfs_path)
                    elif cmd == "rm":  # 删除数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm(dfs_path)
                    elif cmd == "format":  # 格式化DFS
                        response = self.format()
                    elif cmd == "reduce":  # 格式化DFS
                        dfs_path = request[1]
                        response = self.reduce(dfs_path)
                    else:
                        response = "Undefined command: " + " ".join(request)
                    
                    sock_fd.send(bytes(response, encoding='utf-8'))
                except KeyboardInterrupt:
                    break
                finally:
                    sock_fd.close()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            listen_fd.close()
    
    def load(self, dfs_path):
        # 本地路径
        local_path = data_node_dir + dfs_path
        # 读取本地数据
        with open(local_path) as f:
            chunk_data = f.read(dfs_blk_size)
        
        return chunk_data
    
    def store(self, sock_fd, dfs_path):
        # 从Client获取块数据
        chunk_data = recvall(sock_fd)
        # 本地路径
        local_path = data_node_dir + dfs_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 将数据块写入本地文件
        with open(local_path, "wb") as f:
            f.write(chunk_data)
        
        return "Store chunk {} successfully~".format(local_path)
    
    def rm(self, dfs_path):
        local_path = data_node_dir + dfs_path
        rm_command = "rm -rf " + local_path
        os.system(rm_command)
        
        return "Remove chunk {} successfully~".format(local_path)
    
    def format(self):
        format_command = "rm -rf {}/*".format(data_node_dir)
        os.system(format_command)
        
        return "Format datanode successfully~"

    def reduce(self, dfs_path):
        """Compute Sum(X), Sum(X^2) and Count(X) locally"""
        local_path = data_node_dir + dfs_path
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
            })

        return data_pd.to_csv(index=False)


# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
