#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 09:53:51 2018

@author: gavit
"""

import json
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
from plotly.graph_objs import *
py.tools.set_credentials_file(username='fjcadenastsc', api_key='8qk7LoGKn6uJtAQC9Zop')



## Read json files
filename = 'test-lg-g4'  # 'test-lg-g4' 

file_path = './' + filename + '_geowithplaces.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_geo = json.load(json_file)
json_file.close()

file_path = './' + filename + '_wifis.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_wifis = json.load(json_file)
json_file.close()

## Filter invalid samples
data = []
for datapoint in datapoints_geo:
    
    coordinates = datapoint['body']['coordinates']
    time_frame = datapoint['body']['effective_time_frame']['date_time']['$date']/1000 #Time in seconds
    if coordinates['longitude']!=-1 and coordinates['latitude']!=-1 :
        if coordinates['accuracy'] < 50:
            if datapoint['body']['attributes']['speed'] < 2:
                data.append([time_frame, coordinates['longitude'], coordinates['latitude'], coordinates['altitude']])

## Export data to .mat file and plot
data = np.array(data)

scipy.io.savemat(filename+'.mat', {'data': data})

plt.plot(data[:,1], data[:,2], 'bo')
plt.show()


## Parametros modificables

km = 0.4
samp_min = 5

eps_alt = 10
eps_w = 0.001

alpha_alt = 0.25/eps_alt
alpha_w = 0.25/eps_w

##

km_per_radian = 6371
d_min = km / km_per_radian


indexes = []
data_iter = []
clustered_x = []
clustered_y = []

## Compute distances
geo = data[:, 1:3]
geo_matrix_dist = dist.cdist(geo, geo, metric="euclidean")

alt = data[:, 3:4]
alt_dist_matrix = dist.cdist(alt, alt, metric="hamming")

#wifi = data[:, 4:-1]
#wifi_dist_matrix = dist.cdist(wifi, wifi, metric="hamming")

dist_data = geo_matrix_dist * (1 + alpha_alt*(alt_dist_matrix-eps_alt) ) #+ alpha_w*(wifi_dist_matrix-eps_w) )

## DBSCAN algoritm
db = DBSCAN(eps=d_min, metric='precomputed', min_samples=samp_min)
db.fit(dist_data)

labels = db.labels_

unique_labels = set(labels)
unique_labels.remove(-1)
no_clustered_data = np.where(labels == -1)[0]
clustered_data = np.where(labels >= 0)[0]
cluster_list = list(unique_labels)

traces = []


for cluster in cluster_list:
    x1 = [geo[np.where(labels == cluster)[0], 0], cluster]
    y1 = [geo[np.where(labels == cluster)[0], 1], cluster]
    
    traceX = Scattergl(
        x=geo[np.where(labels == cluster)[0], 0],
        y=geo[np.where(labels == cluster)[0], 1],
        mode='markers',
        marker=dict(
            size=5
        )
    )

    traces.append(traceX)
    clustered_x.append(x1)
    clustered_y.append(y1)
    

data_fig = Data(traces)

layout = Layout(
    xaxis=dict(
        range=[-1, 1]
    ),
    yaxis=dict(
        range=[-1, 1]
    )
)

fig = Figure(data=data_fig, layout=layout)

pass

fig = Figure(data=data_fig, layout=layout)

#py.image.save_as(fig, filename=str(km)+ ".png")
#py_off.plot(fig)
scipy.io.savemat(filename + '_clustered.mat', {'x': clustered_x, 'y': clustered_y})


############# INFO ###########
# Number of clusters in labels, ignoring noise if present.
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)

print('\nEstimated number of clusters: %d' % n_clusters_)
print("Silhouette Coefficient: %0.3f"
      % metrics.silhouette_score(geo, labels))

# #############################################################################