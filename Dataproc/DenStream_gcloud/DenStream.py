#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 15:48:32 2017

@author: Gaspar Avit
"""

import sys
import copy
import math
import numpy as np

from sklearn.cluster import DBSCAN

from sample import Sample
from cluster import Cluster
from microCluster import MicroCluster
    
class DenStream():
    
    def __init__(self, lamb, epsilon=1, minPts=5, beta=1, mu=1,\
                numberInitialSamples=40, startingBuffer=None, tp=12):

        ### Algorithm parameters ###  
        self.lamb = lamb
        self.minPts = minPts
        self.beta = beta
        self.numberInitialSamples = numberInitialSamples
        self.buffer = startingBuffer
        self.tp = tp
        self.radiusFactor = 1.2

        ### Check input type: epsilon ### 
        if isinstance(epsilon, int) or isinstance(epsilon, float):
            self.epsilon = epsilon
        elif isinstance(epsilon, str):
            if epsilon == 'auto':
                self.epsilon = 'auto'
        else:
            sys.exit('Error in parameter: epsilon')

        ### Check input type: mu ###
        if isinstance(mu, int) or isinstance(mu, float):
            self.mu = mu
        elif isinstance(mu, str):
            if mu == 'auto':
                self.mu = 'auto'
        else:
            sys.exit('Error in parameter: mu')
        
        ### Running parameters ###
        self.o = False

        ### Real timestamp or steps ###          
        self.currentTimestamp = 0

    
    def runInitialization(self):
        print("\n--------------------INITIALIZATION-------------------\n")
        self.resetLearningImpl()
        self.initialDBScan()
        self.inizialized = True
     
        
    def resetLearningImpl(self):
        
        self.currentTimestamp = 0

        self.inizialized = False
        
        self.pMicroCluster = Cluster()
        self.oMicroCluster = Cluster()
                        
        if isinstance(self.mu, str):
            if self.mu == 'auto':
                self.mu = (1/(1-math.pow(2, -self.lamb)))
                
    def initialDBScan(self):

        db = DBSCAN(eps=self.epsilon, min_samples=self.minPts, algorithm='brute').fit(self.buffer)
        clusters = db.labels_
        self.buffer['clusters'] = clusters
        
        clusterNumber = np.unique(clusters)
        print("\nNumber of initial clusters: ", len(clusterNumber))

        for clusterId in clusterNumber:
            
            if (clusterId != -1):
                
                cl = self.buffer[self.buffer['clusters'] == clusterId]
                cl = cl.drop('clusters', axis=1)
                #sample = Sample(cl.iloc[0].tolist(), self.currentTimestamp)

                mc = MicroCluster(self.currentTimestamp, self.lamb)
                
                for sampleNumber in range(len(cl[1:])):
                    sample = Sample(cl.iloc[sampleNumber].tolist(), self.currentTimestamp)
                    mc.insertSample(sample, self.currentTimestamp)
                    
                self.pMicroCluster.insert(mc)
        
        if isinstance(self.epsilon, str):
            if self.epsilon == 'auto':
                self.epsilon = self.pMicroCluster.clusters[0].radius * self.radiusFactor 
                
    '''
    def initWithoutDBScan(self):
        
        sample = Sample(self.buffer.iloc[0].values, 0)
        sample.setTimestamp(1)
        
        mc = MicroCluster(1, self.lamb)
        
        for sampleNumber in range(0, len(self.buffer)):
            sample = Sample(self.buffer.iloc[sampleNumber].values, sampleNumber)
            sample.setTimestamp(sampleNumber+1)
            mc.insertSample(sample, self.currentTimestamp)
            
        self.pMicroCluster.insert(mc)

        if isinstance(self.epsilon, str):
            if self.epsilon == 'auto':
                self.epsilon = self.pMicroCluster.clusters[0].radius * self.radiusFactor 
    '''
    
    def nearestCluster (self, sample, kind):
        minDist = 0.0
        minCluster = None
        
        if kind == 'cluster':
            clusterList = self.pMicroCluster.clusters
        elif kind == 'outlier':
            clusterList = self.oMicroCluster.clusters
        else:
            sys.exit('Error in choosing kind nearestCluster type: if pMicroCluster or oMicroCluster')
        
        for cluster in clusterList:
            
            if (minCluster == None):
                minCluster = cluster
                minDist = np.linalg.norm(sample.value - cluster.center)

            dist = np.linalg.norm(sample.value - cluster.center)
            dist -= cluster.radius

            if (dist < minDist):
                minDist = dist
                minCluster = cluster
                
        return minCluster
       
    def updateAll(self, mc):
        
        for cluster in self.pMicroCluster.clusters:
            
            if (cluster != mc):
                cluster.noNewSamples()
                
        for cluster in self.oMicroCluster.clusters:
            
            if (cluster != mc):
                cluster.noNewSamples()
        
        
    
    def runOnNewSample(self, sample):

        self.currentTimestamp += 1
        sample.setTimestamp(self.currentTimestamp)

        ### INITIALIZATION PHASE ###
        if not self.inizialized:
            print("---------------------INITIALIZATION-------------------")
            self.buffer.append(sample)
            if (len(self.buffer) >= self.numberInitialSamples):
                self.resetLearningImpl()
                self.initialDBScan()
                self.inizialized = True

        ### MERGING PHASE ###
        else:
            merged = False
            TrueOutlier = True

            if len(self.pMicroCluster.clusters) != 0:
                closestMicroCluster = self.nearestCluster(sample, kind='cluster')
                                
                backupClosestCluster = copy.deepcopy(closestMicroCluster)
                backupClosestCluster.insertSample(sample, self.currentTimestamp)
                
                if (backupClosestCluster.radius <= self.epsilon):

                    closestMicroCluster.insertSample(sample, self.currentTimestamp)
                    merged = True
                    TrueOutlier = False
                    
                    self.updateAll(closestMicroCluster)
                    
            if not merged and len(self.oMicroCluster.clusters) != 0:
                
                closestMicroCluster = self.nearestCluster(sample, kind='outlier')
            
                backupClosestCluster = copy.deepcopy(closestMicroCluster)
                backupClosestCluster.insertSample(sample, self.currentTimestamp)
                
                if (backupClosestCluster.radius <= self.epsilon):
                    closestMicroCluster.insertSample(sample, self.currentTimestamp)
                    merged = True
                    
                    if (closestMicroCluster.weight > self.beta * self.mu):
                        self.oMicroCluster.clusters.pop(self.oMicroCluster.clusters.index(closestMicroCluster))
                        self.pMicroCluster.insert(closestMicroCluster)
                    
                    self.updateAll(closestMicroCluster)
                        
                    
            if not merged:
                newOutlierMicroCluster = MicroCluster(1, self.lamb)
                newOutlierMicroCluster.insertSample(sample, self.currentTimestamp)
                                
                for clusterTest in self.pMicroCluster.clusters:
                    
                    if np.linalg.norm(clusterTest.center-newOutlierMicroCluster.center) < 2 * self.epsilon:
                        TrueOutlier = False

                if TrueOutlier:
                    self.oMicroCluster.insert(newOutlierMicroCluster)
                    self.updateAll(newOutlierMicroCluster)
                else:
                    self.pMicroCluster.insert(newOutlierMicroCluster)
                    self.updateAll(newOutlierMicroCluster)
                
            if self.currentTimestamp % self.tp == 0:
                            
                for cluster in self.pMicroCluster.clusters:

                    if cluster.weight < self.beta * self.mu:
                        self.pMicroCluster.clusters.pop(self.pMicroCluster.clusters.index(cluster))
                        
                for cluster in self.oMicroCluster.clusters:
                    
                    creationTimestamp = cluster.creationTimeStamp
                        
                    xs1 = math.pow(2, -self.lamb*(self.currentTimestamp - creationTimestamp + self.tp)) - 1
                    xs2 = math.pow(2, -self.lamb * self.tp) - 1
                    xsi = xs1 / xs2

                    if cluster.weight < xsi:
                        
                        self.oMicroCluster.clusters.pop(self.oMicroCluster.clusters.index(cluster))
                        
            record = {
                'pMicroClusters': [[y.center[0], y.center[1], y.radius, y.weight, y.creationTimeStamp] for y in self.pMicroCluster.clusters],
                'oMicroClusters': [[y.center[0], y.center[1], y.radius, y.weight, y.creationTimeStamp] for y in self.oMicroCluster.clusters],
                'numberClusters': len(self.pMicroCluster.clusters)
            }

            return record


