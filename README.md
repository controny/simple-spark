# simple-spark

## 一、系统架构

该项目基于简单的分布式文件系统实现spark框架，整体的类图为

![20191215170558.png](https://raw.githubusercontent.com/controny/PicBed/master/images/20191215170558.png)

- **DFSClient**：DFS的客户端，仅进行分布式文件系统的读写操作。
- **NameNode**：统筹数据的读写。
- **DataNode**：进行实际的数据读写与计算。
- **RDD**：记录RDD的相关信息，如操作、子节点、id等。其中还需要定义textFile、map、take等方法，作为提供给用户的接口，创建新的Operation。
- **Operation**：抽象类，其子类应首先包括Transformation和Action类，而后两者又拥有TextFileOp、MapOp、TakeOp等实际操作类，其中定义统一的接口（如`__call__`）以供RDD调用，进行实际的计算。
- **SparkContext**：spark的上下文。继承RDD，作为计算图的root，是一种特殊的RDD。

## 二、安装及使用
```shell
# 下载仓库到本地
git clone https://github.com/controny/simple-spark
cd simple-spark
# 安装所需的python包
python3 scripts/install_all.py
# 一键部署所有节点
python3 scripts/start_all.py
# 一键终止所有节点
python3 scripts/stop_all.py
```

## 三、Git协作方式

```bash
# 提交代码
git add .
git commit -m "[本次代码更新的主要内容]"
# 拉取源仓库上的更新，可能会有冲突需要自行解决
git pull 
# 推送到远程仓库
git push
```
