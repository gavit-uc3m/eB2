#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 11:59:34 2018

@author: gavit
"""

import json
from datetime import datetime
import numpy as np
from time import time



################ DISTANCE GEO ###############
## Read json file
filename = 'test-lg-g4'  # 'test-lg-g4' 

file_path = './' + filename + '_geowithplaces.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_geo = json.load(json_file)
    
json_file.close()


## Extract time, distance and speed vectors
distance_geo = []

for datapoint in datapoints_geo:
    
    schema_id = datapoint['header']['schema_id']['name']
    user = datapoint['header']['user']
    
    attributes = datapoint['body']['attributes']
    time_frame = datetime.fromtimestamp(datapoint['body']['effective_time_frame']['date_time']['$date']/1000) #Time in seconds
    
    distance_geo.append([(time_frame.year, time_frame.month, time_frame.day), attributes['distance']])


## Remove invalid samples
distance_geo = list(filter(lambda x: x[1] != -1, distance_geo))
distance_geo.sort()


for key, values in distance_geo:
    print(key)
########################### AQUI ##############################################################################################################################


################ DISTANCE FITBIT ###############
## Read json file
file_path = './distance_fitbit_lg_g4.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_fitibit = json.load(json_file)
    
json_file.close()


## Extract time, distance and speed vectors
distance_fitibit = []

for datapoint in datapoints_fitibit['activities-distance']:
    
    date = datapoint['dateTime']
    distance_fitibit.append([(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]) ), datapoint['value'] ])

