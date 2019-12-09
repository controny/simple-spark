#!/usr/bin/python
# -*- coding: utf-8 -*-
from rdd import RDD
from uniqueID import *

class SparkContext:
    # confusing with def
    def __init__(self):
        leaf = []

    def end(self,RDD):
        #return ends list
        #[A,B,C]
        if RDD.childs:
            for child in RDD.childs:
                end(child)
        else:
            leaf.append(RDD)
        return leaf