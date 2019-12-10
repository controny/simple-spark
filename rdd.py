#!/usr/bin/python
# -*- coding: utf-8 -*-
from uniqueID import *

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
    def __init__(self,parent=None, operation=None):
        self.parent = parent
        self.operation = operation 
        self.childs = []
        self.ID = next(unique_sequence)

    def flatMap(self, func):
        child = RDD(parent=self, operation=FlatMapOp(func))
        self.childs.append(child)
        if type(operation) = Action:
            operation()
        return child
    
    def Map(self, func):
        child = RDD(parent=self, operation=MapOp(func))
        self.childs.append(child)
        if type(operation) = Action:
            operation()
        return child
    
    def cache(self):
        child = RDD(parent=self, operation="cache")
        self.childs.append(child)
        if type(operation) = Action:
            operation()
        return child
    
    def textFile(self, address):
        child = RDD(parent=self, operation=TextFileOp(address))
        self.childs.append(child)
        if type(operation) = Action:
            operation()
        return child
    