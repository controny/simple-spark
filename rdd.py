#!/usr/bin/python
# -*- coding: utf-8 -*-
from uniqueID import *

class RDD:
# To do list:
# method
# point to parent and child 
# store operation
# unique ID
    def __init__(self,parent=None,operation=None):
        self.parent = parent
        self.operation = operation 
        self.ID = next(unique_sequence)
    def flatMap(self):
        child = RDD(parent=self,operation="flatMap")
        self.child = child
    def Map(self):
        child = RDD(parent=self,operation="Map")
        self.child = child
    def cache(self):
        child = RDD(parent=self,operation="cache")
        self.child = child
    def textFile(self):
        child = RDD(parent=self,operation="textFile")
        self.child = child
    