#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 17:46:51 2017

@author: anr.putina
"""

import math
import numpy as np

def computeReductionFactor(lamb, steps):
    return math.pow(2, -lamb * steps)

class MicroCluster():
    def __init__(self, lamb, creationTimeStamp, long=None, lat=None, radius=None, weight=None, LS1=None, LS2=None, \
                 SS1=None, SS2=None, N=None, dimensions=None):
        
        self.center = [long, lat]
        self.radius = radius
        self.weight = weight
        self.LS = [LS1, LS2]
        self.SS = [SS1, SS2]
        self.creationTimeStamp = creationTimeStamp
        self.N = N
        self.dimensions = dimensions
        self.lamb = lamb
        self.reductionFactor = computeReductionFactor(self.lamb, 1)
                        
    def insertSample(self, sample, timestamp):

        if self.dimensions == None:
            self.dimensions = len(sample.value)

            ### incremental parameteres ###
            self.N = 0
            self.weight = 0
            self.LS = np.zeros(self.dimensions)
            self.SS = np.zeros(self.dimensions)
            self.center = np.zeros(self.dimensions)
            self.radius = 0 

        self.N += 1
        self.updateRealTimeWeight()
        self.updateRealTimeLSandSS(sample)
        
    def updateRealTimeWeight(self):
        
        self.weight *= self.reductionFactor
        self.weight += 1
        
    def updateRealTimeLSandSS(self, sample):
        self.LS = np.multiply(self.LS, self.reductionFactor)
        self.SS = np.multiply(self.SS, self.reductionFactor)
                
        self.LS = self.LS + sample.value
        sample_power = [x**2for x in sample.value]
        self.SS = self.SS + sample_power

        self.center = np.divide(self.LS, float(self.weight))

        LSd = np.power(self.center, 2)
        SSd = np.divide(self.SS, float(self.weight))


        maxRad = np.nanmax(np.sqrt(np.absolute(SSd-LSd)))
                    
        self.radius = maxRad        

    def noNewSamples(self):
        self.LS = np.multiply(self.LS, self.reductionFactor)
        self.SS = np.multiply(self.SS, self.reductionFactor)
        self.weight = np.multiply(self.weight, self.reductionFactor)
                
    def getCenter(self):
        return self.center

    def getRadius(self):
        return self.radius