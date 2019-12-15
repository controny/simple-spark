# -*- coding: UTF-8 -*-
from pyspark import SparkContext 
from pyspark import SparkConf
import random
import sys
import numpy as np
import time
# sys.setrecursionlimit(30000)
start = time.time()
conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)
#聚类中心数
K = 10 
dim = 300
#迭代阈值
convergeDist = 0.5
#初始值
tempDist = 100000
#随机选取 聚类中心
# def random_gen(k):
#   random_list = []
#   for _ in range (k):
#     temp = [] 
#     for i in range (dim):
#       temp.append(random.random()*2-1)
    
#     # temp.append(random.random()*40+20)
#     random_list.append(temp)
#   return random_list
def random_gen(k, points):
    random.shuffle(points)
    random_list = []
    for i in range(k):
        random_list.append(points[i])
    return random_list
# 计算两点的欧式距离 输入为list //用numpy
def distanceSquared(vector1,vector2):
  # dist = 0
  vector1 = np.array(vector1)
  vector2 = np.array(vector2)
  dist = np.sum((vector1 - vector2)**2)
  # for i in range(dim):
  #   dist = dist +(vector1[i]-vector2[i])**2
  # dist = dist**0.5
  return dist
# 计算两点之和 输入list
def addPoints(x,y):
  point = np.array(x[0]) + np.array(y[0]).tolist()
  # point = []
  # for i in range(dim):
  #   point1 = x[0][i]
  #   point2 = y[0][i]
  #   point.append(point1+point2)
  num = x[1] + y[1]
  return  (point, num)
#计算新的聚类中心(取平均)
def average(newpoint):
  point = []
  point = (np.array(newpoint[1][0])/newpoint[1][1]).tolist()
  # for i in range(dim):
  #   point.append(newpoint[1][0][i]/newpoint[1][1])
  #   point.append(point)
  return (newpoint[0],(point,newpoint[1][1]))
#计算一群点中距离某个点最近的点的角标 
def closestPoint(point, centriods):
    vector1 = np.array(point)
    vector1 = np.stack([vector1 for _ in range(K)])
    vector2 = np.array(centriods)
    dist = np.sum((vector1 - vector2)**2, axis=1)
    bestIndex = np.argmin(dist)
    # print(bestIndex)
    # bestIndex = 0
    # closest = distanceSquared(point,centriods[0])
    # for i in range (K):
    #     dist = distanceSquared(point, centriods)
    #     if (dist < closest):
    #         closest = dist
    #         bestIndex = i
    return bestIndex
#获取数据源，并将数据解析为点的元组
textfile = sc.textFile("/wc2.txt")
#print(textfile.take(2))
line = textfile.map(lambda line: line.split(" "))
#lines = line.take(2)
#print(lines)
fields = line.map(lambda fields:[float(x) for x in fields[1:]])
#field = fields.take(1)
#print(field)
points  = fields.filter(lambda point: any(point))

# point =points.take(1)
# #获取初始聚类中心
kpoints = random_gen(K, points.filter(lambda x: random.random()<0.1).collect())
# kpoints = random_gen(K)
# print(kpoints)
# print(len(kpoints[0]))
# closest = points.map(lambda points:(closestPoint(points,kpoints),(points,1)))  
# closests = closest.take(2) 
# # print(closests)
# pointStats = closest.reduceByKey(lambda x,y:addPoints(x,y))
# pointStat = pointStats.take(2) 
# print(closests)
# newPoints = pointStats.map(lambda newpoint:average(newpoint))
# newPoint = newPoints.take(1)
# print(len(newPoint[0][1][0]))
while (tempDist > convergeDist):
  #找到距离每个点最近的点的角标，并记录(index, (p, 1))
    closest = points.map(lambda points:(closestPoint(points,kpoints),(points,1)))
    # closests = closest.take(20)
    # print("closests",len(closests))
  #根据角标，聚合周围最近的点，并把周围的点相加
    pointStats = closest.reduceByKey(lambda x,y:addPoints(x,y))
    # pointStat =pointStats .take(20)
    # print(pointStat)
    # print("pointStat",len(pointStat))
  #计算周围点的新中心点
    #newPoints = pointStats.map(lambda newpoint:(newpoint[0],(newpoint[1][0][0]/newpoint[1][1],newpoint[1][0][1]/newpoint[1][1]),newpoint[1][1])).collect()
    newPoints = pointStats.map(lambda newpoint:average(newpoint)).collect()
    newPoints = sorted(newPoints, key=lambda x: x[0])
    # print(pointStat)
    # print("newPoints",len(newPoints))
    tempDist = 0.0
    for i in range(K):
        tempDist = tempDist + distanceSquared(kpoints[i], newPoints[i][1][0])
    print("Distance between iterations:{}".format(tempDist))
    for i in range(K):
        kpoints[i] = newPoints[i][1][0]
#print("Final K points:{}".format(kpoints))
print(len(kpoints))
with open('result.txt','w') as f:
  for x in kpoints:
    f.write(' '.join([str(y) for y in x])+'\n')
f.close()
end = time.time()
print("Time is {}".format(end-start))