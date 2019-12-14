# coding=utf-8
import math
import os
import socket

import numpy as np
from multiprocessing import Process
import pandas as pd
import traceback

from common import *
from utils import *


def handle(sock_fd, address, namenode):
    print("Connection from : ", address)
    try:
        # 获取请求方发送的指令
        # request = str(recv_msg(sock_fd), encoding='utf-8')
        request = deserialize(recv_msg(sock_fd))
        request = request.split()  # 指令之间使用空白符分割
        print("Request: {}".format(request))

        cmd = request[0]  # 指令第一个为指令类型

        if cmd == "ls":  # 若指令类型为ls, 则返回DFS上对于文件、文件夹的内容
            dfs_path = request[1]  # 指令第二个参数为DFS目标地址
            response = namenode.ls(dfs_path)
        elif cmd == "get_fat_item":  # 指令类型为获取FAT表项
            dfs_path = request[1]  # 指令第二个参数为DFS目标地址
            response = namenode.get_fat_item(dfs_path)
        elif cmd == "new_fat_item":  # 指令类型为新建FAT表项
            dfs_path = request[1]  # 指令第二个参数为DFS目标地址
            num_blk = int(request[2])
            response = namenode.new_fat_item(dfs_path, num_blk)
        elif cmd == "rm_fat_item":  # 指令类型为删除FAT表项
            dfs_path = request[1]  # 指令第二个参数为DFS目标地址
            response = namenode.rm_fat_item(dfs_path)
        elif cmd == "format":
            response = namenode.format()
        else:  # 其他位置指令
            response = "Undefined command: " + " ".join(request)

        print("Response: {}".format(response))
        send_msg(sock_fd, serialize(response))
    except IndexError:
        # Ignore empty request
        pass
    except Exception:  # 如果出错则打印错误信息
        traceback.print_exc()
    finally:
        sock_fd.close()  # 释放连接


class NameNode:
    def run(self):  # 启动NameNode
        # 创建一个监听的socket
        listen_fd = socket.socket()
        # reuse a local socket in TIME_WAIT state, without waiting for its natural timeout to expire
        # refer to https://stackoverflow.com/questions/29217502/socket-error-address-already-in-use/29217540
        listen_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", name_node_port))
            listen_fd.listen(5)
            print("Name node listening at port %s" % name_node_port)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("connected by {}".format(addr))

                process = Process(target=handle, args=(sock_fd, addr, self))
                process.start()

        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        finally:
            listen_fd.close()  # 释放连接
    
    def ls(self, dfs_path):
        local_path = name_node_dir + dfs_path
        # 如果文件不存在，返回错误信息
        if not os.path.exists(local_path):
            return "No such file or directory: {}".format(dfs_path)
        
        if os.path.isdir(local_path):
            # 如果目标地址是一个文件夹，则显示该文件夹下内容
            dirs = os.listdir(local_path)
            response = " ".join(dirs)
        else:
            # 如果目标是文件则显示文件的FAT表信息
            with open(local_path) as f:
                response = f.read()
        
        return response
    
    def get_fat_item(self, dfs_path):
        # 获取FAT表内容
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        died_hosts = self.get_died_hosts()
        if len(died_hosts) != 0:
            print('Died hosts: %s' % died_hosts)
            # Delete those died hosts in the FAT to be returned
            for idx, row in response.iterrows():
                for host in died_hosts:
                    response.loc[idx, 'host_names'] = row['host_names'].replace(host, '').replace(',,', ',').strip(',')
        return response.to_csv(index=False)
    
    def new_fat_item(self, dfs_path, num_blk):
        data_pd = pd.DataFrame(columns=['blk_no', 'host_names'])

        for i in range(num_blk):
            blk_no = i
            num_replication = min(dfs_replication, len(host_list))  # in case that the number of hosts is less
            host_names = np.random.choice(host_list, size=num_replication, replace=False)
            host_names_str = ','.join(host_names)
            print('host name', host_names_str)
            data_pd.loc[i] = [blk_no, host_names_str]
        
        # 获取本地路径
        local_path = name_node_dir + dfs_path
        
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)
    
    def rm_fat_item(self, dfs_path):
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        os.remove(local_path)
        return response.to_csv(index=False)
    
    def format(self):
        format_command = "rm -rf {}/*".format(name_node_dir)
        os.system(format_command)
        return "Format namenode successfully~"

    def get_died_hosts(self):
        """Return a list of died hosts"""
        results = []
        for host in host_list:
            sock = socket.socket()
            try:
                sock.connect((host, data_node_port))
                cmd = "ping"
                send_msg(sock, serialize(cmd))
                response_msg = recv_msg(sock)
                if str(response_msg, encoding='utf-8') != '200':
                    raise ValueError
            except:
                results.append(host)
            finally:
                sock.close()
        return results


# 创建NameNode并启动
name_node = NameNode()
name_node.run()
