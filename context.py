#!/usr/bin/python
# -*- coding: utf-8 -*-
from rdd import RDD
from uniqueID import *

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