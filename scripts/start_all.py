import sys
sys.path.append('./')
from common import *
import os

cur_dir = os.getcwd()

# start current node as name node
command_start = 'python3 %s/name_node.py &' % cur_dir
print('command to start name node: %s' % command_start)
os.system(command_start)

for slave in host_list:
    print('sending DFS codes to slave [%s]' % slave)
    command_send_codes =\
        'ssh {0} "mkdir -p {1}"; scp -r */*.py */*.sh requirements.txt {0}:{1} > /dev/null'\
        .format(slave, cur_dir)
    os.system(command_send_codes)
    # also install pip requirements
    install_codes =\
        'ssh {0} pip install -i https://pypi.tuna.tsinghua.edu.cn/simple some-package -r {1}/requirements.txt --user'\
            .format(slave, cur_dir)
    os.system(install_codes)
    command_start = 'ssh %s "python3 %s/data_node.py " &' % (slave, cur_dir)
    print('trying to start data node [%s]' % slave)
    os.system(command_start)
