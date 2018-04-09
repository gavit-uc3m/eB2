
import json

from pymongo import MongoClient
import scipy.io as sio
import numpy as np
import os
from bson.json_util import dumps
import urllib
from lxml import html
from bson import json_util




def get_datapoints(db_name, users, time_period, data_type):
    print ('downloading datapoints from user '+users[0]+' and type '+data_type)
    client = MongoClient('eb2-01', 27017)
    db = client[db_name]
    collection = db['datapoints']

    if data_type == "imu":
        schema_id = ["acceleration_time_series", "gyroscope_time_series", "magnetic_time_series","angular_velocity_time_series","magnetic_field_time_series"]
    elif data_type == "geo":
        schema_id = ["Location of Google", "geo_google"]
    elif data_type == "geowithplaces":
        schema_id = ["geo_google"]
    elif data_type == "activity":
        schema_id = ["physical_activity","activity"]
    elif data_type == "google_activity":
        schema_id = ["google_activity"]
    elif data_type == "callreg":
        schema_id = ["call"]
    elif data_type == "audiofeatures":
        schema_id = ["audiofeatures"]
    elif data_type == "bluetooth":
        schema_id = ["bluetooth"]
    elif data_type == "wifis":
        schema_id = ["wifi"]
    elif data_type == "steps":
        schema_id = ["step_count"]
    elif data_type == "appusage":
        schema_id = ["appsusage"]
    elif data_type == "actigraphy":
        schema_id = ["actigraphy"]
    elif data_type == "light":
        schema_id = ["light"]
    if users[0] == '':
        query = {
                "header.schema_id.name": {"$in": schema_id},
        }

    else:
        query = {
            "$and": [
                {"header.schema_id.name": {"$in": schema_id}},
                {"header.user": {"$in": users}}
            ]
        }

    array = []
    obj_arr= []

    cursor = collection.find(query)
    for i in cursor:
        array.append(i)

    for datapoint in array:
        obj_arr.append(datapoint)



    print (obj_arr)
    with open(users[0] + '_' + data_type + '.json', 'w') as outfile:
        json.dump(obj_arr, outfile, ensure_ascii=False, default=json_util.default)
        
#    sio.savemat(users[0]+'/' +users[0] + '' + data_type + 'cells.json', {data_type: str(obj_arr)})
