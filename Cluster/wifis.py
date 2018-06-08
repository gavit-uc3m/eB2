#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 12:14:59 2018

@author: gavit
"""

import json
from datetime import datetime
import numpy as np
from time import time



## Read json files
filename = 'test-lg-g4'  # 'test-lg-g4' 

file_path = './' + filename + '_wifis.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_wifis = json.load(json_file)
json_file.close()

data_wifi = []
hash_list = []

t0 = time()


for datapoint in datapoints_wifis:
    
    wifis = datapoint['body']['wifis']
    wifi_list = []
    wifi_list_id = []
    
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
                 
    time_frame = datetime.fromtimestamp(datapoint['body']['effective_time_frame']['date_time']['$date']/1000)
    data_wifi.append([(time_frame.year, time_frame.month, time_frame.day), (time_frame.hour, time_frame.minute), wifi_list_id ])

data_wifi.sort(reverse=True)


for idx in range(0, len(data_wifi)):    
    mask = np.zeros((len(hash_list), 1))
    mask[data_wifi[idx][2]] = 1 
    data_wifi[idx][2] = mask
    
del mask, idx, hash_name, power, wifi_hash, wifi_list, wifi_list_id, wifis, item

    
duration = time() - t0
print("\nComputed in %f seconds" %duration)


    
    
    
