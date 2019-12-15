#!/usr/bin/python
# -*- coding: utf-8 -*-
from uniqueID import *
from operation import *

class RDD:
# To do list:
# method ,not sure
# point to parent and child ,Done 
# store operation ,Done 
# unique ID  ,Done
# ---------Discussion-----------
# should we store partitions?
# type() is Action?
# execute from the head to now
# operation collect and take  
    # partition to store 
    # so FAT format get from textFile store in RDD   
    def __init__(self,parent=None, operation=None):
        self.parent = parent
        self.operation = operation 
        self.childs = []
        self.ID = next(unique_sequence)
        self.partition = None

    # def flatMap(self, func):
    #     child = RDD(parent=self, operation=FlatMapOp(func))
    #     self.childs.append(child)
    #     if type(operation) == Action:
    #         self.execute()
    #     return child
    
    def map(self, func):
        child = RDD(parent=self, operation=MapOp(func))
        self.childs.append(child)
        return child

    def reduceByKey(self, func):
        child = RDD(parent=self, operation=ReduceByKeyOp(func))
        self.childs.append(child)
        return child

    def filter(self,func):
        child = RDD(parent=self, operation=FilterOp(func))
        self.childs.append(child)
        return child

    def take(self, num):
        operation=TakeOp(num)
        child = RDD(parent=self, operation=operation)
        self.childs.append(child)
        if isinstance(operation,Action):
            value = child.execute()
        return value    
    # def cache(self):
    #     child = RDD(parent=self, operation="cache")
    #     self.childs.append(child)
    #     if type(operation) == Action:
    #         self.execute()
    #     return child
    
    def textFile(self, address):
        child = RDD(parent=self, operation=TextFileOp(address))
        self.childs.append(child)
        return child

    def execute(self):
        # execute from root to now
        # 1. find the top and kepp the steps 
        # 2. using the steps to execute to now
        now = self
        step = 0
        execution_list = []
        # find the root 
        while (not isinstance(now, SparkContext)):
            execution_list.append(now)
            now = now.parent
            step += 1
        # execute from root to now
        i = 0
        while(i < step):
            RDD_now = execution_list.pop()
            if isinstance(RDD_now.operation, ReduceByKeyOp):
                # reduceByKey will update partition table
                self.partition = RDD_now.operation(self.partition, i)
            elif not isinstance(RDD_now.operation, TextFileOp):
                value = RDD_now.operation(self.partition, i)
            else:
                self.partition = RDD_now.operation(i)
            i += 1
        return value


class SparkContext(RDD):
    # confusing with def
    # print() messeage of action
    def __init__(self):
        super(SparkContext,self).__init__()
        self.leaf = []

    def end(self):
        #return ends list
        #[A,B,C]
        if self.childs:
            for child in self.childs:
                end(child)
        else:
            self.leaf.append(self)
        return self.leaf


# Test
if __name__ == '__main__':
    try:
        times = 0
        temp = []
        while (times<100):
            sc = SparkContext()
            text = sc.textFile('/test.txt')
            mapped = text.map(lambda x: (x, 1))
            reduced = mapped.reduceByKey(lambda a, b: a + b)
            # filterdone = reduced.filter(lambda x: x[0] == 'American')
            take_res = reduced.take(30)
            take_res = [str(x) for x in take_res]
            take_res.sort()
            print('[take]\n%s' % '\n'.join(take_res))
            if temp != take_res and times != 0:
                print("it's diff in times : {} ".format(times))
                print('[this time]\n%s' % '\n'.join(take_res))
                break 
            temp = take_res
            times += 1
    finally:
        # clear memory of all nodes whatever
        Operation.clear_memory()
