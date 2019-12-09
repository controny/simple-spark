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
    def __init__(self,parent=None,operation=None):
        self.parent = parent
        self.operation = operation 
        self.childs = []
        self.ID = next(unique_sequence)
    def flatMap(self):
        child = RDD(parent=self,operation="flatMap")
        self.childs.append(child)
        return child
    
    def Map(self):
        child = RDD(parent=self,operation="Map")
        self.childs.append(child)
        return child
    
    def cache(self):
        child = RDD(parent=self,operation="cache")
        self.childs.append(child)
        return child
    
    def textFile(self):
        child = RDD(parent=self,operation="textFile")
        self.childs.append(child)
        return child
    