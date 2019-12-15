# coding=utf-8
import sys
import numpy as np
from rdd import *
import random
from utils import *
#for test
def llamb(keypoint):
    return(closestPoint(points,kpoints),(points,1)) 

if __name__ == '__main__':
    try:
        K = 10
        dim = 300
        #迭代阈值
        convergeDist = 0.5
        #初始值
        tempDist = 100000
        collect = 400000
        sc = SparkContext()
        textfile = sc.textFile("/word.txt")
        
        line = textfile.map(lambda line: line.split(" "))
        fields = line.map(lambda fields:[float(x) for x in fields[1:]])
        points  = fields.filter(lambda point: any(point))
        kpoints = random_gen(K, points.filter(lambda x: random.random()<0.1).take(collect))
        while (tempDist > convergeDist):
            closest = points.map(lambda points, kpoints=kpoints,K=K:(closestPoint(points,kpoints,K),(points,1)))
            
            logger.debug(closest.map(lambda x: x[0]).take(collect))
            pointStats = closest.reduceByKey(lambda x,y:addPoints(x,y))
            logger.debug("Here {}".format(len(pointStats.take(K))))
            newPoints = pointStats.map(lambda newpoint:average(newpoint)).take(collect)
            newPoints = sorted(newPoints, key=lambda x: x[0])
            tempDist = 0.0
            for i in range(K):
                tempDist = tempDist +distanceSquared(kpoints[i], newPoints[i][1][0])
            logger.info("Distance between iterations:{}".format(tempDist))
            for i in range(K):
                kpoints[i] = newPoints[i][1][0]
        logger.info(len(kpoints))
        with open('result.txt','w') as f:
            for x in kpoints:
                f.write(' '.join([str(y) for y in x])+'\n')
            f.close()
    finally:
        # clear memory of all nodes whatever
        Operation.clear_memory()
