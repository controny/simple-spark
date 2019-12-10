import sys
sys.path.append('./')
from common import *
import os

cur_dir = os.getcwd()
command_kill_pattern = 'kill $(lsof -t -i:%d)'

# stop name node
command_kill = command_kill_pattern % name_node_port
print('trying to kill name node')
os.system(command_kill)

# stop all data nodes
command_kill = command_kill_pattern % data_node_port
for slave in host_list:
    command_remote_kill = 'ssh %s \'%s\'' % (slave, command_kill)
    print('trying to kill data node [%s]' % slave)
    os.system(command_remote_kill)
