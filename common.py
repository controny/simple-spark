# coding=utf-8
dfs_replication = 3
dfs_blk_size = 4096 * 1024

# NameNode和DataNode数据存放位置
name_node_dir = "/home/dsjxtjc/2019270049/MyDFS/dfs/name"
data_node_dir = "/home/dsjxtjc/2019270049/MyDFS/dfs/data"

data_node_port = 21234# DataNode程序监听端口
name_node_port = 21235# NameNode监听端口

# 集群中的主机列表
host_list = ['thumm02', 'thumm03', 'thumm04', 'thumm05']
# host_list = ['thumm01']  # DEBUG
name_node_host = "localhost"

BUF_SIZE = 4096 * 2
