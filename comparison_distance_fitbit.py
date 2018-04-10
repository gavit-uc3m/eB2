#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 11:59:34 2018

@author: gavit
"""

import json
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np



################ DISTANCE GEO ###############
## Read json file
nombre = 'asun'
#nombre = 'lg-g4'
filename = 'test-' + nombre 


file_path = './' + filename + '_geowithplaces.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_geo = json.load(json_file)
    
json_file.close()


## Extract time, distance and speed vectors
distance_geo = []
geo_data = []

for datapoint in datapoints_geo:
    
    schema_id = datapoint['header']['schema_id']['name']
    user = datapoint['header']['user']
    
    attributes = datapoint['body']['attributes']
    time_frame = datetime.fromtimestamp(datapoint['body']['effective_time_frame']['date_time']['$date']/1000) #Time in seconds
    
    if attributes['speed'] < 3 and datapoint['body']['coordinates']['accuracy'] < 100:
        distance_geo.append([(time_frame.year, time_frame.month, time_frame.day), attributes['distance']/1000]) #Distance in km
        geo_data.append([(time_frame.year, time_frame.month, time_frame.day), (time_frame.hour, time_frame.minute), attributes['speed'], attributes['distance'], datapoint['body']['coordinates']['accuracy']]) #Distance in km


## Remove invalid samples
distance_geo = list(filter(lambda x: x[1] > 0, distance_geo))
distance_geo = list(filter(lambda x: x[0] != (1970, 1, 1), distance_geo))
distance_geo.sort()

dist_geo = {}

for idx, value in distance_geo:
    found = False
    for i in dist_geo:
        if idx == i:
            found = True
    if found:
        dist_geo[idx] = value + dist_geo[idx]
    else:
        dist_geo[idx] = value
    
dist_geo = np.array(list(dist_geo.items()))

geo_data = list(filter(lambda x: x[2] != -1, geo_data))
geo_data = list(filter(lambda x: x[0] != (1970, 1, 1), geo_data)) 
geo_data.sort(reverse=True)


################ DISTANCE FITBIT ###############
## Read json file
file_path = './distance_fitbit_' + nombre + '.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_fitibit = json.load(json_file)
    
json_file.close()


## Extract time and distance
distance_fitibit = []

for datapoint in datapoints_fitibit['activities-distance']:
    
    date = datapoint['dateTime']
    distance_fitibit.append([(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]) ), float(datapoint['value']) ])

dist_fitibit = np.array(list(filter(lambda x: x[1]>0, distance_fitibit)))
t = np.linspace(1, min(len(dist_fitibit), len(dist_geo)), num=min(len(dist_fitibit), len(dist_geo)))

dist_plot_fit = dist_fitibit[-len(t):len(dist_fitibit),1]
dist_plot_geo = dist_geo[-len(t):len(dist_geo),1]

plt.axis([0, len(t)+1, 0, max(max(dist_plot_fit), max(dist_plot_geo))+1])
plt.stem(t, dist_plot_fit)
plt.plot([t[0], t[-1]], [np.mean(dist_plot_fit), np.mean(dist_plot_fit)], 'b--')

plt.stem(t, dist_plot_geo,'r')
plt.plot([t[0], t[-1]], [np.mean(dist_plot_geo), np.mean(dist_plot_geo)], 'r--')

plt.legend(['Fitbit distance', 'App distance'])
plt.show()


