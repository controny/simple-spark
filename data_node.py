# coding=utf-8
import os
import socket
from multiprocessing import Process, Manager
import pandas as pd
import numpy as np
import traceback
import pickle

from common import *
from utils import *


def handle(sock_fd, address, datanode, memory):
    print("Connection from : ", address)
    try:
        # 获取请求方发送的指令
        raw_request = recv_msg(sock_fd)
        request = str(raw_request, encoding='utf-8')
        request = request.split()  # 指令之间使用空白符分割
        print(request)

        cmd = request[0]  # 指令第一个为指令类型

        try:
            if cmd in ['store']:
                response = getattr(datanode, cmd)(sock_fd, *request[1:])
            elif cmd in ['map']:
                response = getattr(datanode, cmd)(memory, sock_fd, *request[1:])
            elif cmd in ['take', 'text_file', 'clear_memory']:
                response = getattr(datanode, cmd)(memory, *request[1:])
            else:
                response = getattr(datanode, cmd)(*request[1:])
        except Exception as e:
            traceback.print_exc()
            response = str(e)

        print('response for command [%s]: %s' % (cmd, response))
        if type(response) is not bytes:
            response = bytes(response, encoding='utf-8')
        send_msg(sock_fd, response)
    except KeyboardInterrupt:
        sock_fd.close()
        return
    except Exception:
        traceback.print_exc()
    finally:
        sock_fd.close()


class DataNode:

    def run(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        # reuse a local socket in TIME_WAIT state, without waiting for its natural timeout to expire
        # refer to https://stackoverflow.com/questions/29217502/socket-error-address-already-in-use/29217540
        listen_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # use shared memory
        # note that there exists a bug before python 3.6:
        # 'Cannot modify dictionaries inside dictionaries using Managers'
        # refer to https://bugs.python.org/issue6766
        manager = Manager()
        memory_partitions = manager.dict()
        memory_progress = manager.dict()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(5)
            print('data node listening at port %s' % data_node_port)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))

                process = Process(target=handle, args=(sock_fd, addr, self, [memory_partitions, memory_progress]))
                process.daemon = True
                process.start()

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

    def text_file(self, memory, dfs_base_path, blk_no, step):
        """Load file into memory in the form of lines"""
        print('performing [text_file] operation for bulk ' + blk_no)
        dfs_path = '%s.blk%s' % (dfs_base_path, blk_no)
        local_path = dfs2local_path(dfs_path)
        with open(local_path) as f:
            chunk_data = f.read(dfs_blk_size)
            memory[0][blk_no] = chunk_data.split('\n')
        self.update_progress(memory, blk_no, step)
        return "Load text file successfully~"

    def take(self, memory, blk_no, num, step):
        """Take lines from chunk data in memory"""
        print('performing [take] operation for bulk ' + blk_no)
        self.check_progress(memory, blk_no, step)
        lines = memory[0][blk_no]
        num = int(num)
        if num != -1:
            # -1 means take all data
            lines = lines[:num]
        serialized = serialize(lines)
        self.update_progress(memory, blk_no, step)
        return serialized

    def map(self, memory, sock_fd, blk_no, step):
        print('performing [map] operation for bulk ' + blk_no)
        self.check_progress(memory, blk_no, step)
        func = deserialize(recv_msg(sock_fd))
        memory[0][blk_no] = list(map(func, memory[0][blk_no]))
        self.update_progress(memory, blk_no, step)
        return "Map data successfully~"

    def clear_memory(self, memory):
        for sub_memory in memory:
            sub_memory.clear()
        return "Clear memory successfully~"

    def ping(self):
        return '200'

    def update_progress(self, memory, blk_no, step):
        memory[1][blk_no] = int(step)
        print('updated progress: ', memory[1])

    def check_progress(self, memory, blk_no, step):
        print('check progress: ', memory[1])
        memory_progress = memory[1]
        # The operation must wait when
        # 1. The bulk has not been loaded
        # 2. The current progress of the bulk >= step-1
        while memory_progress.get(blk_no) is None or memory_progress[blk_no] < int(step)-1:
            print('blk %s: waiting for the preceding operations to finish' % blk_no)
            time.sleep(0.05)

# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
