# 使用方法

### 一、系统架构

#### 1.1. 介绍

该DFS实现了简单分布式文件系统的功能，包含了NameNode、DataNode、CLient三部分，其中NameNode负责记录文件块的位置（FAT表）， DataNode负责数据的存储与读取，而Client则是用户与DFS交互的接口。

该DFS主要实现了ls / copyFromLocal /copyToLocal功能, 大家可以在这个基础上完成以下功能：

- 实现复数个块副本。 这个DFS的dfs_replication为1，也就是一个数据块存储在一台主机上，大家需要修改common.py里的配置，以及NameNode、DataNode、CLient中对于的部分，实现存储多副本块存储。

- 利用该框架实现 均值和方差统计 功能，数据可以自己采集或随机生成，无固定格式要求，数据文件大小需要大于1G，小于5G。

- （加分项）有一定的任务错误处理机制（比如某台节点宕机或者出现数据块丢失）

在写实验报告时需要详细阐述你的设计，包括整体设计思想，系统框架，数据分割方案，任务分配和整合方案等细节，体现你对DFS和MapReduce思想的理解。


#### 1.2. 目录结构
- MyDFS : 根目录
    - dfs : DFS文件夹，用于存放DFS数据
        - name : 存放NameNode数据
        - data : 存放DataNode数据
    -test : 存放测试样例
        -test.txt
    - common.py  : 全局变量
    - name_node.py : NameNode程序
    - data_node.py : DataNode程序
    - client.py : Client程序，用于用户与DFS交互

#### 1.3. 模块功能

- name_node.py
    - 保存文件的块存放位置信息
    - 获取文件/目录信息
    - get_fat_item： 获取文件的FAT表项
    - new_fat_item： 根据文件大小创建FAT表项
    - rm_fat_item： 删除一个FAT表项
    - format: 删除所有FAT表项

- data_node.py
    - load 加载数据块
    - store 保存数据块
    - rm 删除数据块
    - format 删除所有数据块

- client.py
    - ls : 查看当前目录文件/目录信息
    - copyFromLocal : 从本地复制数据到DFS
    - copyToLocal ： 从DFS复制数据到本地
    - rm ： 删除DFS上的文件
    - format ：格式化DFS

### 二、操作步骤

0. 进入MyDFS目录
```
$ cd MyDFS
``
1. 启动NameNode

```sh
$ python3 name_node.py
```

2. 启动DataNode

```
$ python3 data_node.py
```

3. 测试指令

- ls <dfs_path> : 显示当前目录/文件信息
- copyFromLocal <local_path> <dfs_path> : 从本地复制文件到DFS
- copyToLocal <dfs_path> <local_path> : 从DFS复制文件到本地
- rm <dfs_path> : 删除DFS上的文件
- format : 格式化DFS


首先从本地复制一个文件到DFS

```
$ python3 client.py -copyFromLocal test/test.txt /test/test.txt
File size: 8411
Request: new_fat_item /test/test.txt 8411
Fat:
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219
```

启动blk_no为块号， host_name为该数据块存放的主机名字，blk_size为块的大小

查看DFS的根目录内容

```sh
$ python3 client.py -ls /test
test.txt
```

如果ls后面跟的是文件，可以看到该文件FAT信息

```sh
$ python3 client.py -ls /test/test.txt
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219
```

接着再从DFS复制到本地，存储为test2.txt

```sh
$ python3 client.py -copyToLocal /test/test.txt test/test2.txt
Request: get_fat_item /test/test.txt
Fat:
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219
```

校验前后文件是否一致

```sh
$ diff test/test.txt test/test2.txt
$
```

可以看到前后文件一致

接着测试删除文件

```sh
$ python3 client.py -rm /test/test.txt
Request: rm_fat_item /test/test.txt
Fat:
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219

b'Remove chunk ./dfs/data/test/test.txt.blk0 successfully~'
b'Remove chunk ./dfs/data/test/test.txt.blk1 successfully~'
b'Remove chunk ./dfs/data/test/test.txt.blk2 successfully~'
```

再次查看/test文件夹
```
$ python3 client.py -ls /test

```
原先的test.txt已删除

接着测试format
```sh
$ python3 client.py -format
format
Format namenode successfully~
Format datanode successfully~
```

查看根目录
```sh
$ python3 client.py -ls /

```
所有文件/文件夹均被删除