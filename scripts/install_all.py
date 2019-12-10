import sys
sys.path.append('./')
from common import *
import os

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
        'ssh {0} pip install -i https://pypi.tuna.tsinghua.edu.cn/simple some-package -r {1}/requirements.txt --user &'\
            .format(slave, cur_dir)
    os.system(install_codes)
