
# coding: utf-8

from __future__ import absolute_import
from pyspark.sql import SQLContext, functions, SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from time import time, mktime
from sklearn.cluster import DBSCAN
#import sklearn
import copy
import datetime
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

clust_schema = StructType([StructField('user', StringType(), False),
                          StructField('creationTimeStamp', IntegerType(), False),
                          StructField('longitude', FloatType(), False),
                          StructField('latitude', FloatType(), False),
                          StructField('radius', FloatType(), False),
                          StructField('weight', FloatType(), False),
                          StructField('LS1', FloatType(), False),
                          StructField('LS2', FloatType(), False),
                          StructField('SS1', FloatType(), False),
                          StructField('SS2', FloatType(), False),
                          StructField('N', IntegerType(), False),
                          StructField('dimensions', IntegerType(), False),
                          StructField('altitude', FloatType(), True),
                          StructField('wifi', StringType(), True),
                          StructField('bluetooth', StringType(), True),
                          StructField('activities', StringType(), True),
                          StructField('places', StringType(), True),
                          StructField('type_cluster', StringType(), False),
                         ])

df_last_mod = sqlContext.read.jdbc(url=url, table='last_modification', properties=properties)


#Filter the service and the type of data to be procesed
last_mod_actual = (df_last_mod
                   .rdd
                   .filter(lambda x: x[-1] == 'clustering')
                  )

#Take the data that is not going to be procesed
if last_mod_actual.count() == 0:
    last_mod_aux = df_last_mod
else:
    last_mod_aux = df_last_mod.filter( df_last_mod.data_type != 'clustering' )

df_clusters = sqlContext.read.jdbc(url=url, table='clusters', properties=properties)

#Broadcast the date of last modification
broadcast_data_type = sc.broadcast('clustering')
broadcast_date = sc.broadcast(last_mod_actual.collect())
broadcast_clusters = sc.broadcast(df_clusters.rdd.collect())



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
where longitude is not null
and longitude > -180
and longitude < 180
and latitude is not null
and latitude > -90
and latitude < 90
and speed < 0.5
and accuracy < 200
"""
clean_data = sqlContext.sql(sql_query)
data_rdd = clean_data.rdd

## ----------------------------------------------------------------------



## --------------------- Extraction and processing of data ----------------
def header_map(x):
    date_all = datetime.datetime.strptime(x['creation_datetime'].split(' U',)[0].split('.',)[0], "%Y-%m-%d %H:%M:%S")
    day = date_all.date()
    user = x['user']
    service = x['service']
    
    last_mod = [y for y in broadcast_date.value if (y[1] == user)]

    if len(last_mod) == 0:
        if (day < (datetime.date.today() - datetime.timedelta(days=2))):
            return (service, user, day, broadcast_data_type.value)
        else: 
            return
    else:
        date_mod = datetime.datetime.strptime(last_mod[0][2], "%Y-%m-%d").date()
        if (day > date_mod) & (day < (datetime.date.today() - datetime.timedelta(days=2))):
            return (service, user, day, broadcast_data_type.value)
        else:
            return

def places_map(x):

    date_all = datetime.datetime.strptime(x['creation_datetime'].split('U',)[0], "%Y-%m-%d %H:%M:%S ")
    day = date_all.date()
    user = x['user']
    places =  x['places'][0]['placetype']
    social_lit = [3, 4, 9, 13, 15, 23, 27, 45, 48,  ]

    return ( (user, day), [x for x in places if x in social_list])

def weekly_group(x):
    user = x[0][0]
    day = x[0][1]
    val = x[1]

    return ((user, day.year, day.isocalendar()[1]), val )


'''
header = (data_rdd
          .map(header_map)
          .filter(lambda x: x is not None)
          .distinct()
         )

if header.count() == 0:
    last_mod_rdd = last_mod_actual
    print('Header vacio')
else:
    last_mod_rdd = (header
                    .map(lambda x: ((x[0], x[1], x[3]), x[2]))
                    .groupByKey()
                    .map(lambda x: (x[0], sorted(list(x[1]))))
                    .map(lambda x: (x[0], x[1][-1]))
                    .map(lambda x: (x[0][0], x[0][1], x[1], x[0][2]))
                   )
df1 = sqlContext.createDataFrame(last_mod_actual.map(lambda x: (x[0], x[1], datetime.datetime.strptime(x[2], "%Y-%m-%d").date(), x[3]) ), header_schema)
df2 = sqlContext.createDataFrame(last_mod_rdd, header_schema)
unmodified_df = df1.join(df2, (df1.service == df2.service) & (df1.user == df2.user), how="leftanti")
last_mod = unmodified_df.union(df2)
'''


social_places = (data_rdd
                 .map(places_map)
                 .filter(lambda x: len(x[1]) > 0)
                 .map(lambda x: (x[0], x[1][0]))
                 .map(weekly_group)
                 .combineByKey(lambda v: [v], lambda x,y: x+[y], lambda x,y: x+y)
                 .map(lambda x: (datetime.strptime(str(x[0][1])+ '-' + str(x[0][2]) + '-1', "%Y-%W-%w").date(), x[1]) )
                 .map(lambda x: [[x[0].year, x[0].month, x[0].day], x[1]])
                 )


print("\n\n")
print(social_places.take(1))
print("\n")



