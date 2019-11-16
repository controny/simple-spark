from common import *
import os

cur_dir = os.getcwd()
# TODO kill process by port using `lsof -i:[port]`
# Use `tr -s " "` to squeezes the spaces of ps output together, refer to https://stackoverflow.com/a/15643939
command_kill_pattern = 'kill $(ps -ef | grep "python3 %s" | grep -v grep | tr -s " " | cut -d " " -f 2)'

# stop name node
command_kill = command_kill_pattern % os.path.join(cur_dir, 'name_node.py')
print('trying to kill name node')
os.system(command_kill)

# stop all data nodes
command_kill = command_kill_pattern % os.path.join(cur_dir, 'data_node.py')
for slave in host_list:
    command_remote_kill = 'ssh %s \'%s\'' % (slave, command_kill)
    print('trying to kill data node [%s]' % slave)
    os.system(command_remote_kill)
