import sys
sys.path.append('./')
from common import *
import os

local_install_code = \
    'pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt --user &'
os.system(local_install_code)

cur_dir = os.getcwd()

for slave in host_list:
    # install pip requirements for all nodes
    print('sending DFS codes to slave [%s]' % slave)
    command_send_codes =\
        'scp requirements.txt {0}:{1} > /dev/null'\
        .format(slave, cur_dir)
    os.system(command_send_codes)
    print('installing requirements for slave [%s]' % slave)
    install_codes =\
        'ssh {0} pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple -r {1}/requirements.txt --user &'\
            .format(slave, cur_dir)
    os.system(install_codes)
