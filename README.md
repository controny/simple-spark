# simple-spark

### 一、系统架构

#### 1.1. 介绍

该项目基于简单的分布式文件系统实现spark框架，大致架构为
![20191204213256.png](https://raw.githubusercontent.com/controny/PicBed/master/images/20191204213256.png)

#### 1.2. 目录结构（待更新）
- simple-spark : 根目录
    - dfs : DFS文件夹，用于存放DFS数据
        - name : 存放NameNode数据
        - data : 存放DataNode数据
    - common.py  : 全局变量
    - name_node.py : NameNode程序
    - data_node.py : DataNode程序
    - client.py : Client程序，用于用户与DFS交互

#### 1.3. 模块功能（待更新）

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

### 二、安装及使用

```shell
# 下载仓库到本地
git clone https://github.com/controny/simple-spark
cd simple-spark
# 安装所需的python包
pip3 install --user -r requirements.txt
# 一键部署所有节点
python3 start_all.py
# 一键终止所有节点
python3 stop_all.py
```

### 三、Git协作方式

```bash
# 提交代码
git add .
git commit -m "[本次代码更新的主要内容]"
# 拉取源仓库上的更新，可能会有冲突需要自行解决
git pull 
# 推送到远程仓库
git push
```