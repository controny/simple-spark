from common import *
import os

cur_dir = os.getcwd()

# start current node as name node
command_start = 'python3 %s/name_node.py &' % cur_dir
print('command to start name node: ', command_start)
os.system(command_start)

for slave in host_list:
    print('sending DFS codes to slave [%s]' % slave)
    command_send_codes = 'ssh %s "mkdir -p %s"; scp *.py %s:%s > /dev/null' % (slave, cur_dir, slave, cur_dir)
    os.system(command_send_codes)
    command_start = 'ssh %s "python3 %s/data_node.py " &' % (slave, cur_dir)
    print('trying to start data node [%s]' % slave)
    os.system(command_start)
