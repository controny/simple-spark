# coding=utf-8
dfs_replication = 3  # number of replications
dfs_blk_size = 4096 * 1024  # size of eack bulk

# data directories of nodes
name_node_dir = "path/to/info"
data_node_dir = "path/to/data"

data_node_port = 11332  # listeing port of Data Nodes
name_node_port = 21332  # listeing port of Name Nodes

# host names of the cluster
host_list = ['host1', 'host2', 'host3', 'host4']
name_node_host = "localhost"

BUF_SIZE = 4096 * 2
