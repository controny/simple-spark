#!/usr/bin/python
# -*- coding: utf-8 -*-
from uniqueID import *

class RDD:
# To do list:
# method
# point to parent and child 
# store operation
# unique ID
    def __init__(self,child,parent=None):
        self.parent = parent 
        self.child = child
        self.ID = next(unique_sequence)
    def flatMap(self):
        self.operation = "flatMap" 
    def Map(self):
        self.operation = "Map" 
    def cache(self):
        self.operation = "cache"
    def textFile(self):
        self.operation = "textFile"
    