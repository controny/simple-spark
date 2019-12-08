#!/usr/bin/python
# -*- coding: utf-8 -*-
from rdd import RDD
from uniqueID import *

class SparkContext(RDD):
    # confusing with def
    def read(self,file_adress):
       pass