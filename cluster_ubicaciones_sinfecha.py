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
        if coordinates['accuracy'] < 90:
            if datapoint['body']['attributes']['speed'] < 0.5:
#                if 'places' in datapoint['body'].keys():
#                    print(datapoint['body']['places'])
                data_geo.append([time_frame, coordinates['longitude'], coordinates['latitude'], coordinates['altitude']])

data_geo = np.array(data_geo)
geo_ts = np.array(data_geo[:,0])
#geo_ts = np.flipud(np.sort(np.array(data_geo[:,0])))


## Load wifi data and preprocess it
data_wifi = []
hash_list = []

for datapoint in datapoints_wifis:
    
    wifis = datapoint['body']['wifis']
    wifi_list = []
    wifi_list_id = []
    time_frame = datapoint['body']['effective_time_frame']['date_time']['$date']/1000 #Time in seconds
        
    for wifi_hash in wifis:
        hash_name = wifi_hash.split('-')[0][0:-1]
        power = wifi_hash.split(' ')[1]
        wifi_list.append([power, hash_name])
    wifi_list.sort()
    
    if len(wifi_list) >= 5:
        wifi_list = list(np.array(wifi_list)[0:5,1])
    elif len(wifi_list) == 0:
        wifi_list = []
    else:
        wifi_list = list(np.array(wifi_list)[0:len(wifi_list),1])
     
    for item in wifi_list:
        if item not in hash_list:
            hash_list.append(item)
        wifi_list_id.append(hash_list.index(item))
    
    data_wifi.append([time_frame, wifi_list_id])
        
#data_wifi.sort(reverse=True)
wifi_ts = np.array([item[0] for item in data_wifi])

    #wifi_ts = np.flipud(np.sort(np.array(data_wifi)[:,0]))
    #geo_ts = geo_ts - (geo_ts[0]-wifi_ts[0])
    #
    #geo_ts_date = []
    #
    #for item in geo_ts:
    #    geo_ts_date.append([(datetime.fromtimestamp(item).year, datetime.fromtimestamp(item).month, \
    #                         datetime.fromtimestamp(item).day), (datetime.fromtimestamp(item).hour, datetime.fromtimestamp(item).minute)])
    #
    #wifi_ts_date = []
    #
    #for item in wifi_ts:
    #    wifi_ts_date.append([(datetime.fromtimestamp(item).year, datetime.fromtimestamp(item).month, \
    #                          datetime.fromtimestamp(item).day), (datetime.fromtimestamp(item).hour, datetime.fromtimestamp(item).minute)])


wifi_matrix = []
for idx in range(0, len(data_wifi)): 
    mask = np.zeros((len(hash_list), 1))
    mask[data_wifi[idx][1]] = 1 
    data_wifi[idx][1] = mask
    wifi_matrix.append(mask.reshape(1,len(mask)))
    
wifi_matrix = np.stack(wifi_matrix)[:,0,:]
    


# TimeStamp Preprocessing
dims_wifi = wifi_matrix.shape[1]
wifi_matrix_processed = []

for idx in range(0, len(geo_ts)):
    
    ts = geo_ts[idx]

    wifi_idx = np.where(((ts - 60) <= wifi_ts) * (wifi_ts <= (ts + 60)))[0]
    if len(wifi_idx) > 0:
        wifi_values = wifi_matrix[wifi_idx, :]
    else:
        wifi_values = np.zeros((1, dims_wifi))
    wifi_matrix_processed.append(wifi_values)
wifi_matrix_processed = np.stack(wifi_matrix_processed)[:,0,:]

del mask, idx, hash_name, power, wifi_hash, wifi_list, wifi_list_id, wifis, item
## End wifi data processing


## Export geo data to .mat file and plot
scipy.io.savemat(filename+'.mat', {'data': data_geo})
plt.plot(data_geo[:,1], data_geo[:,2], 'bo')
plt.show()


## Parametros modificables

km = 0.4
samp_min = 5

eps_alt = 50
eps_w = 0.002

alpha_alt = 0.25/eps_alt
alpha_w = 0.25/eps_w

##

km_per_radian = 6371
d_min = km / km_per_radian


indexes = []
clustered_x = []
clustered_y = []

print('\nPreprocessing completed')
t0 = time()

## Compute distances
geo = data_geo[:, 1:3]
geo_dist_matrix = dist.cdist(geo, geo, metric="euclidean")

alt = data_geo[:, 3]
alt = alt.reshape((len(alt),1))
alt_dist_matrix = dist.cdist(alt, alt, metric="euclidean")

wifi_dist_matrix = dist.cdist(wifi_matrix_processed, wifi_matrix_processed, metric="hamming")

dist_data = geo_dist_matrix * (1 + alpha_alt*(alt_dist_matrix-eps_alt) + alpha_w*(wifi_dist_matrix-eps_w) )

print('\nDistances computed')
duration = time() - t0
print("\nDistances computed in %f seconds" %duration)


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