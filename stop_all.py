from common import *
import os

cur_dir = os.getcwd()
# Use `tr -s " "` to squeezes the spaces of ps output together, refer to https://stackoverflow.com/a/15643939
command_kill_pattern = 'kill $(ps -ef | grep "python3 %s" | grep -v grep | tr -s " " | cut -d " " -f 2)'

# stop name node
command_kill = command_kill_pattern % os.path.join(cur_dir, 'name_node.py')
print('command to kill name node: ', command_kill)
os.system(command_kill)

# stop all data nodes
command_kill = command_kill_pattern % os.path.join(cur_dir, 'data_node.py')
for slave in host_list:
    command_remote_kill = 'ssh %s \'%s\'' % (slave, command_kill)
    print('command to kill data node [%s]: %s' % (slave, command_remote_kill))
    os.system(command_remote_kill)
