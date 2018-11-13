
# coding: utf-8

from __future__ import absolute_import
from pyspark.sql import SQLContext, functions, SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from time import time, mktime
from sklearn.cluster import DBSCAN
from scipy.interpolate import interp1d
from operator import itemgetter
from itertools import groupby


#import sklearn
import copy
import datetime as dt
import json
import pprint
import subprocess
import pyspark
import numpy as np
import pandas as pd
import math
import itertools
import scipy.io


print("\n\nInicio codigo\n")

t0 = time() #To measure whole processing time

sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

spark = SparkSession \
    .builder \
    .appName("Spark processing") \
    .getOrCreate()

sc.addPyFile("gs://dataproc-1d944e68-ce97-4314-bbd1-366adb951fce-europe-west1/Denstream/sample.py")
sc.addPyFile("gs://dataproc-1d944e68-ce97-4314-bbd1-366adb951fce-europe-west1/Denstream/microCluster.py")


bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input'.format(bucket)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)


## ---------- Reading from last_modification table -------------------

url = "jdbc:mysql://35.195.77.246/test-node"
user = "admin"
password = "1b93e9058ff1d3e4b7b466ba92e7b22e"
driver = "com.mysql.jdbc.Driver"
properties = {
    "user": user,
    "password": password,
    "driver": driver,
    "useSSL": "false"
    }

header_schema = StructType([StructField('service', StringType(), True),
                StructField('user', StringType(), True),
                StructField('date', DateType(), True),
                StructField('data_type', StringType(), False)
                ])


df_last_mod = sqlContext.read.jdbc(url=url, table='last_modification', properties=properties)


#Filter the service and the type of data to be procesed
last_mod_actual = (df_last_mod
                   .rdd
                   .filter(lambda x: x[-1] == 'activity')
                  )

#Take the data that is not going to be procesed
if last_mod_actual.count() == 0:
    last_mod_aux = df_last_mod
else:
    last_mod_aux = df_last_mod.filter( df_last_mod.data_type != 'activity' )


#Broadcast the date of last modification
broadcast_data_type = sc.broadcast('activity')
broadcast_date = sc.broadcast(last_mod_actual.collect())


'''
print('\nbroadcast_date: ')
print(broadcast_date.value)
print('\nlast_mod_aux: ')
print(last_mod_aux.show(10000))
print('\nbroadcast_clusters: ')
print(broadcast_clusters.value)
print('\n')
'''


## -------------------------------------------------------------------



## --------------------- Reading from BigQuery ------------------------
t_bigq = time() #To measure time

conf = {
    # Input Parameters.
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': project,
    'mapred.bq.input.dataset.id': 'Testbigquery',
    'mapred.bq.input.table.id': 'Datapoints',
    "mapred.bq.input.sharded.export.enable": "false"
}

# Load data in from BigQuery.
data_table = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf).map(lambda x: x[1])


duration = time() - t_bigq
print("\nBigQuery table correctly read in %0.2f seconds\n" %duration)

data = spark.read.json(data_table)
data.createOrReplaceTempView("data")
sql_query = """
SELECT *
from data
where activity is not null
"""

clean_data = sqlContext.sql(sql_query)
data_rdd = clean_data.rdd

## ----------------------------------------------------------------------




## --------------------- Extraction and processing of data ----------------
def header_map(x):
    date_all = dt.datetime.strptime(x['creation_datetime'].split(' U',)[0].split('.',)[0], "%Y-%m-%d %H:%M:%S")
    day = date_all.date()
    user = x['user']
    service = x['service']
    
    last_mod = [y for y in broadcast_date.value if (y[1] == user)]

    if len(last_mod) == 0:
        if (day < (dt.date.today() - dt.timedelta(days=2))):
            return (service, user, day, broadcast_data_type.value)
        else: 
            return
    else:
        date_mod = dt.datetime.strptime(last_mod[0][2], "%Y-%m-%d").date()
        if (day > date_mod) & (day < (dt.date.today() - dt.timedelta(days=2))):
            return (service, user, day, broadcast_data_type.value)
        else:
            return

def tilting_map(x):
    date_all = dt.datetime.strptime(x['creation_datetime'].split(' U',)[0].split('.',)[0], "%Y-%m-%d %H:%M:%S")
    day = date_all.date()
    moment = date_all.time()
    user = x['user']
    activity = sorted(x['activity'], key=itemgetter(0))
    if len(activity) > 0:
      if str(activity[-1]['name']) in ['tilting', 'walking', 'running']:
        return ((user, day), (moment, 1))
      else:
        return ((user, day), (moment, 0))


    '''

    last_mod = [y for y in broadcast_date.value if (y[1] == user) ]

    if len(last_mod) == 0:
        if (day < (datetime.date.today() - datetime.timedelta(days=2))):
            return (user, (timestamp, float(x['longitude']), float(x['latitude'])))
        else:
            return
    else:
        day_last_mod = datetime.datetime.strptime(last_mod[0][2], "%Y-%m-%d").date()
        if ((day > day_last_mod) & (day < (datetime.date.today() - datetime.timedelta(days=2)))):
            return (user, (timestamp, float(x['longitude']), float(x['latitude'])))
        else:
            return
    '''

    
def timestampToTime(x):
    
    hours = int(math.floor(x/3600))
    residual = x-hours*3600
    minutes = int(math.floor(residual/60))
    seconds = int(math.floor(residual - minutes*60))
    
    return dt.time(hours, minutes, seconds)

def actig_preprocessing(x):

    header = x[0]
    data = x[1]
    
    points_list = [(x[0].hour*3600+x[0].minute*60+x[0].second, x[1]) for x in data]
    new_axis = [ x for x in range(0,24*3600,300) ]
    ordered_list = np.ones((len(new_axis), 1))

    x_range = [ x for x in range(0,24*3600,300) ]
    for point in points_list:
        tresh = [True if x > point[0] else False for x in x_range ]
        idx = next( (idx for idx,x in enumerate(tresh) if x), -1)
        
        if idx != -1:
            ordered_list[idx] = point[1]
    
    return (header, ordered_list)


def compute_standing(x):

    data = x[1]
    standing = 0

    for val in data:
      if ( (val[-1].hour*60+val[-1].minute) - (val[0].hour*60+val[0].minute)) >= 30:
        standing += 1

    return (x[0], standing)



'''

header = (data_rdd
          .map(header_map)
          .filter(lambda x: x is not None)
          .distinct()
         )

'''



standing_periods = (data_rdd
                 .map(tilting_map)
                 .filter(lambda x: x is not None)
                 .combineByKey(lambda v: [v], lambda x,y: x+[y], lambda x,y: x+y)
                 .map(lambda x: (x[0],  sorted(x[1], key=itemgetter(0))))
                 .map(lambda x: (x[0], [( [x[0] for x in group]) for key, group in groupby(x[1], key=itemgetter(1)) if key == 1 ]) )
                 .map(compute_standing)
                 )



print("\n\n")
print(standing_periods.take(20))
print("\n")

