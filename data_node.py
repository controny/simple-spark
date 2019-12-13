# coding=utf-8
import os
import sys
import socket
from multiprocessing import Process, Manager, managers
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
            elif cmd in ['map', 'local_reduce_by_key', 'store_reduced_data', 'transfer_reduced_data']:
                response = getattr(datanode, cmd)(memory, sock_fd, *request[1:])
            elif cmd in ['load', 'rm', 'format', 'ping']:
                response = getattr(datanode, cmd)(*request[1:])
            else:
                response = getattr(datanode, cmd)(memory, *request[1:])
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
        memory_buffer = manager.dict()
        memory_middle_result = manager.list()
        memory = [memory_partitions, memory_progress, memory_buffer, memory_middle_result]
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(5)
            print('data node listening at port %s' % data_node_port)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))

                process = Process(target=handle, args=(sock_fd, addr, self, memory))
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

    def ping(self):
        return '200'

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

    def local_reduce_by_key(self, memory, sock_fd, step):
        """
        1. Reduce in each bulk;
        2. Reduce all bulks in this machine
        """
        print('performing [local_reduce_by_key] operation in step %s' % step)
        partitions = memory[0]
        raw_func = recv_msg(sock_fd)
        func = deserialize(raw_func)
        # store some variables for later usage
        buffer = memory[2]
        buffer['func'] = raw_func
        buffer['step'] = step
        buffer['data_flag'] = len(host_list)
        jobs = []

        def handle(blk_no):
            self.check_progress(memory, blk_no, step)
            partitions[blk_no] = reduce_by_key(partitions[blk_no], func)
            print('reduce blk', blk_no)

        for blk_no in partitions.keys():
            process = Process(target=handle, args=blk_no)
            process.start()
            jobs.append(process)

        # wait for all processes to finish
        for job in jobs:
            job.join()
        all_values = sum(partitions.values(), [])
        # use the buffer to store the result
        local_res = reduce_by_key(all_values, func)
        buffer['local_reduce'] = local_res

        return "Local reduce by key successfully~"

    def transfer_reduced_data(self, memory, sock_fd):
        """Transfer locally-reduced data to the corresponding hosts defined by hash function"""
        buffer = memory[2]
        buffer['sock_fd'] = sock_fd
        # wait for local reduce to finish
        while memory[2].get('local_reduce') is None:
            print('waiting for local reduce to finish')
            time.sleep(0.05)

        num_reducer = len(host_list)
        # pack the elements of the same target host and send together
        to_transfer = {host: [] for host in host_list}
        for element in memory[2]['local_reduce']:
            target_host = host_list[self.hash_key(element, num_reducer)]
            to_transfer[target_host].append(element)

        def handle(target_host, data):
            sock = socket.socket()
            sock.connect((target_host, data_node_port))
            message = ''
            while message != '200':
                request = "store_reduced_data"
                print('[store_reduced_data] connect ' + target_host)
                send_msg(sock, bytes(request, encoding='utf-8'))
                send_msg(sock, serialize(data))
                message = str(recv_msg(sock), encoding='utf-8')
            sock.close()

        jobs = []
        for host in host_list:
            process = Process(target=handle, args=(host, to_transfer[host]))
            process.start()
            jobs.append(process)

        for job in jobs:
            job.join()

        # tell all hosts that it has finished transferring data
        for host in host_list:
            sock = socket.socket()
            sock.connect((host, data_node_port))
            print('[global_reduce_by_key] connect ' + host)
            send_msg(sock, bytes('global_reduce_by_key', encoding='utf-8'))
            sock.close()

        return '200'

    def store_reduced_data(self, memory, sock_fd):
        """Store received reduced data in the buffer of memory"""
        data = deserialize(recv_msg(sock_fd))
        memory[3].extend(data)
        return '200'

    def global_reduce_by_key(self, memory):
        """
        1. Check if all hosts have finished transferring data
        2. Combine all received data, perform reducing again and finally update partition table
        """
        buffer = memory[2]
        buffer['data_flag'] -= 1
        print('data_flag:', buffer['data_flag'])

        if buffer['data_flag'] == 0:
            global_reduce = memory[3]
            result = reduce_by_key(global_reduce, deserialize(buffer['func']))
            # make sure not to reassign a normal list to memory
            for idx, element in enumerate(result):
                global_reduce[idx] = element
            self.update_memory_with_new_partitions(memory)
            # convert new partition to dict and send back to client
            send_msg(buffer['sock_fd'], serialize(dict(memory[0])))
            return '200'

        return '404'

    def hash_key(self, element, num_reducers):
        return hash(list(element.keys())[0]) % num_reducers

    def clear_memory(self, memory):
        for sub_memory in memory:
            if isinstance(sub_memory, managers.ListProxy):
                sub_memory[:] = []
            else:
                sub_memory.clear()
        return "Clear memory successfully~"

    def update_memory_with_new_partitions(self, memory):
        """Substitute original partition table with reduced results"""
        partitions, progress, buffer, middle_results = memory

        partitions.clear()
        partitions.update(self.split_data_into_bulks(middle_results))

        step = buffer['step']
        for blk_no in partitions.keys():
            self.update_progress(memory, blk_no, step)

    def split_data_into_bulks(self, data):
        res = {}
        blk_no = 0
        split_start = 0
        for i in range(len(data)):
            cur_blk = data[split_start:i]
            next_blk = data[split_start:i+1]
            if sys.getsizeof(cur_blk) < dfs_blk_size < sys.getsizeof(next_blk) or i == len(data)-1:
                res[str(blk_no)] = cur_blk
                split_start = i
                blk_no += 1
        return res

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
            # print('blk %s: waiting for the preceding operations to finish' % blk_no)
            time.sleep(0.05)

# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
