#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 16:49:54 2018

@author: gavit
"""

import json
import numpy as np
import scipy.io


## Read json files
filename = 'pruebas7'
file_path = './' + filename + '.json'
with open(file_path, encoding='utf-8-sig') as json_file:
    datapoints_geo = json.load(json_file)
json_file.close()


## Load data and filter invalid samples
data_geo = []
for datapoint in datapoints_geo:
    if datapoint['longitude'] is not None:
        if datapoint['longitude'] != -1:
            data_geo.append([float(datapoint['longitude']), float(datapoint['latitude'])])

      
data = np.array(data_geo)
scipy.io.savemat(filename+'.mat', {'data': data})






