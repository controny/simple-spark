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


# Test
if __name__ == '__main__':
    sc = SparkContext()
    text = sc.textFile('/wc_dataset.txt')
    mapped = text.map(lambda x: {x: 1})
    take_res = mapped.take(10)
    take_res = [str(x) for x in take_res]
    print('[take]\n%s' % '\n'.join(take_res))
