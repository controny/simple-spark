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

    def flatMap(self, func):
        child = RDD(parent=self, operation=FlatMapOp(func))
        self.childs.append(child)
        if type(operation) == Action:
            self.execute()
        return child
    
    def Map(self, func):
        child = RDD(parent=self, operation=MapOp(func))
        self.childs.append(child)
        if type(operation) == Action:
            self.execute()
        return child
    
    def cache(self):
        child = RDD(parent=self, operation="cache")
        self.childs.append(child)
        if type(operation) == Action:
            self.execute()
        return child
    
    def textFile(self, address):
        child = RDD(parent=self, operation=TextFileOp(address))
        self.childs.append(child)
        if type(operation) == Action:
            self.execute()
        return child

    def execute(self):
        # execute from root to now
        # 1. find the top and kepp the steps 
        # 2. using the steps to execute to now
        now = self
        step = 1
        execution_list = []
        # find the root 
        while (now.parent):
            execution_list.append(now)
            now = now.parent
            step += 1
        # execute from root to now
        while(step):
            RDD_now = execution_list.pop()
            if type(RDD_now.operation) is not TextFileOp:
                value = RDD_now.operation(RDD_now.partition)
            else:
                self.partition = RDD_now.operation()
            step -= 1
        return value
    