#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 11:35:30 2018

@author: gavit
"""


import json
from datetime import datetime
import numpy as np
from time import time
from sklearn.cluster import DBSCAN, KMeans
from sklearn import metrics, preprocessing
import scipy.spatial.distance as dist
import scipy.io
import matplotlib.pyplot as plt


import plotly.offline as py_off
import plotly.plotly.plotly as py
from plotly.graph_objs import Scattergl, Layout, Figure, Data
py.tools.set_credentials_file(username='fjcadenastsc', api_key='8qk7LoGKn6uJtAQC9Zop')



## Read json files
filename = 'test-lg-g4'

file_path = './' + filename + '_geowithplaces.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_geo = json.load(json_file)
json_file.close()

file_path = './' + filename + '_wifis.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_wifis = json.load(json_file)
json_file.close()

## Load data and filter invalid samples
data_geo = []
for datapoint in datapoints_geo:
    
    coordinates = datapoint['body']['coordinates']
    time_frame = datapoint['body']['effective_time_frame']['date_time']['$date']/1000 #Time in seconds
    if coordinates['longitude']!=-1 and coordinates['latitude']!=-1 :
        if coordinates['accuracy'] < 100:
            if datapoint['body']['attributes']['speed'] < 270:
                data_geo.append([time_frame, coordinates['longitude'], coordinates['latitude'], datapoint['body']['attributes']['speed']])
                

data_geo = np.array(data_geo);
data_geo_sorted = data_geo[data_geo[:,0].argsort()];
data_geo = data_geo_sorted[-1100:-900,:]
geo_ts = np.array(data_geo[:,0])


plt.plot(data_geo[:,1], data_geo[:,2], 'b')
plt.show()
scipy.io.savemat('path1.mat', {'data': data_geo})

#
### K-means speed clustering
#
#x1 = data_geo[:,3] < 1
x2 =  dist.cdist(data_geo[:,1:3], data_geo[:,1:3], metric="euclidean")
geo_dist_vector = x2.diagonal(1);
plt.plot(geo_dist_vector, 'b')
plt.show()

scipy.io.savemat('geo_dist_matrix.mat', {'data': x2})



#X_tr = data_geo[:,3]
#X = np.transpose(np.vstack((X_tr, y_tr)))

