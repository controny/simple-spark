#!/usr/bin/python
# -*- coding: utf-8 -*-
import random 

def uniqueid(): 
    seed = random.getrandbits(16) 
    while True: 
     yield seed 
     seed += 1 

unique_sequence = uniqueid() 