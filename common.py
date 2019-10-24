# coding=utf-8
dfs_replication = 1
dfs_blk_size = 4096  # * 1024

# NameNode和DataNode数据存放位置
name_node_dir = "./dfs/name"
data_node_dir = "./dfs/data"

data_node_port = 11332  # DataNode程序监听端口
name_node_port = 21332  # NameNode监听端口

# 集群中的主机列表
# host_list = ['localhost']
host_list = ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05']
name_node_host = "localhost"

BUF_SIZE = dfs_blk_size * 2
