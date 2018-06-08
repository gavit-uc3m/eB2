#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 22 12:05:21 2018

@author: gavit
"""


import json
import pandas as pd
from datetime import datetime
import numpy as np
from time import time
from sklearn.cluster import DBSCAN
from sklearn import metrics
import scipy.spatial.distance as dist
import scipy.io
import matplotlib.pyplot as plt

import plotly.offline as py_off
import plotly.plotly.plotly as py
from plotly.graph_objs import Scattergl, Layout, Figure, Data
py.tools.set_credentials_file(username='fjcadenastsc', api_key='8qk7LoGKn6uJtAQC9Zop')

from sample import Sample
from DenStream import DenStream
from statistics import Statistics

from groundTruth import groundTruth

def normalize_matrix(df):
    return (df - df.mean())/df.std()


## Read json files
filename = 'test-lg-g4'

file_path = './' + filename + '_geowithplaces.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_geo = json.load(json_file)
json_file.close()


## Load data and filter invalid samples
data_geo = []
for datapoint in datapoints_geo:
    
    coordinates = datapoint['body']['coordinates']
    time_frame = datapoint['body']['effective_time_frame']['date_time']['$date']/1000 #Time in seconds
    if coordinates['longitude']!=-1 and coordinates['latitude']!=-1 :
        if coordinates['accuracy'] < 90:
            if datapoint['body']['attributes']['speed'] < 0.5:
#                if 'places' in datapoint['body'].keys():
#                    print(datapoint['body']['places'])
                if coordinates['altitude'] != 0:
                    data_geo.append([time_frame, coordinates['longitude'], coordinates['latitude'], coordinates['altitude']])
                else:
                    data_geo.append([time_frame, coordinates['longitude'], coordinates['latitude'], 600])
                    

data_geo = np.array(data_geo)
times = np.array(data_geo[:,0])

geo = data_geo[:, 1:3]
geo_dist_matrix = dist.cdist(geo, geo, metric="euclidean")
index = ['Row'+str(i) for i in range(1, len(geo_dist_matrix)+1)]
columns = np.zeros(len(geo_dist_matrix)+1, dtype = 'string')
columns[0:-2] = 'values'
columns[-1] = 'clusters'

geo_dist_matrix_df = pd.DataFrame(geo_dist_matrix, index=index, columns=columns)

dfNormalized = normalize_matrix(geo_dist_matrix)

sampleSkip = 40
bufferDf = dfNormalized[0:sampleSkip]
testDf = dfNormalized[sampleSkip:]

den = DenStream(lamb=0.03, epsilon='auto', beta=0.03, mu='auto', startingBuffer=bufferDf, tp=36)
den.runInitialization()

outputCurrentNode = []
startingSimulation = time.time()
for sampleNumber in range(len(testDf)):
    sample = testDf.iloc[sampleNumber]
    result = den.runOnNewSample(Sample(sample.values, times.iloc[sampleNumber]))
    outputCurrentNode.append(result)
### END SIMULATION ###
print (time() - startingSimulation)

df['result'] = [False] * sampleSkip + outputCurrentNode

truth = groundTruth()
truth.simulationBGP_CLEAR_Second_DATASET()
truth.simulationBGP_CLEAR2_CLEAR()

statistics = Statistics(1, truth)
results = statistics.getNodeResult(df, times, kMAX=5)

statistics.getPrecisionRecallFalseRate(results, kMAX=5, plot=True)
statistics.getDelay(results, kMAX=5, plot=True, samplingRate=4)
