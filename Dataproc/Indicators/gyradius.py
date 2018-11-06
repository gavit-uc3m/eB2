
# coding: utf-8

from __future__ import absolute_import
from pyspark.sql import SQLContext, functions, SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from time import time, mktime
from sklearn.cluster import DBSCAN
from operator import itemgetter
from itertools import groupby

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

#Broadcast the date of last modification
broadcast_data_type = sc.broadcast('clustering')
broadcast_date = sc.broadcast(last_mod_actual.collect())

'''
print('\nbroadcast_date: ')
print(broadcast_date.value)
print('\nlast_mod_aux: ')
print(last_mod_aux.show(10000))
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
where longitude is not null
and longitude > -180
and longitude < 180
and latitude is not null
and latitude > -90
and latitude < 90
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

def coord_map(x):

    date_all = datetime.datetime.strptime(x['creation_datetime'].split('U',)[0], "%Y-%m-%d %H:%M:%S ")
    day = date_all.date()
    timestamp = int(date_all.strftime("%s"))
    user = x['user']

    last_mod = [y for y in broadcast_date.value if (y[1] == user) ]

    if len(last_mod) == 0:
        if (day < (datetime.date.today() - datetime.timedelta(days=2))):
            return ((user, day), (timestamp, float(x['longitude']), float(x['latitude'])))
        else:
            return
    else:
        day_last_mod = datetime.datetime.strptime(last_mod[0][2], "%Y-%m-%d").date()
        if ((day > day_last_mod) & (day < (datetime.date.today() - datetime.timedelta(days=2)))):
            return ((user, day), (timestamp, float(x['longitude']), float(x['latitude'])))
        else:
            return

def compute_gyradius(x):
    data = x[1]        
    day_data = np.array(sorted(data, key=lambda x: x[0]))
    geo_data = day_data[:,1:]
    
    ## Compute gyradius
    center = [np.mean(geo_data[:,0]), np.mean(geo_data[:,1]) ] 
    gyradius = np.sqrt( sum(( np.sqrt( ( (geo_data[:,0]-center[0])**2 + (geo_data[:,1]-center[1])**2 ).astype(float)) )**2) / len(geo_data) )

    return (x[0], gyradius)



def list_to_array(x):
    output_list = -1*(np.ones((1,48)))
    for idx, value in enumerate(x[1]):
        if value[1] > 0.0:
            output_list[0,int(value[0])] = value[1]
    if len(x[0]) == 4:
        out = ([x[0][3]], output_list.tolist()[0], [x[0][0], x[0][1], x[0][2], 'mean'])
    else:
        out = (output_list.tolist()[0], [x[0][0], x[0][1], x[0][2], 'mean'])
    return list(y for x in out for y in x)


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

last_mod.show(100000)
'''

gyradius_rdd = (data_rdd
                 .map(coord_map)
                 .filter(lambda x: x is not None)
                 .filter(lambda x: x[1][1] != -1.0)
                 .filter(lambda x: x[1][2] != -1.0)
                 .combineByKey(lambda v: [v], lambda x,y: x+[y], lambda x,y: x+y)
                 .map(compute_gyradius)
                 )


print("\n\n")
print(gyradius_rdd.take(10))
print("\n")


exit() ###################################
##########################################


## -------------------------------------------------------------------


## ------------------- Saving to MySQL database ----------------------

## WRITE IN "clusters" TABLE

pCluster_df = sqlContext.createDataFrame(pCluster_rdd, clust_schema)
unmodified_p_df = pCluster_df.join(df_clusters, (pCluster_df.user == df_clusters.user) & \
                  (pCluster_df.type_cluster == df_clusters.type_cluster) , how='leftanti')
pCluster_df_out =  unmodified_p_df.union(pCluster_df).registerTempTable("pCluster_df")
pCluster = sqlContext.table("pCluster_df")
sqlContext.cacheTable("pCluster_df")
pCluster.show(10000)

oCluster_df = sqlContext.createDataFrame(oCluster_rdd, clust_schema)
unmodified_o_df = oCluster_df.join(df_clusters, (oCluster_df.user == df_clusters.user) & \
                  (oCluster_df.type_cluster == df_clusters.type_cluster) , how='leftanti')
oCluster_df_out =  unmodified_o_df.union(oCluster_df).registerTempTable("oCluster_df")
oCluster = sqlContext.table("oCluster_df")
sqlContext.cacheTable("oCluster_df")
oCluster.show(10000)



pCluster.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="clusters",
      user=user,
      password=password).mode('overwrite').save()

oCluster.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="clusters",
      user=user,
      password=password).mode('append').save()

print('\n\nClusters properly saved in Cloud SQL\n\n')

# Update last modification

union_header = (last_mod.union(last_mod_aux)
                .registerTempTable("union_header")
               )

dd = sqlContext.table("union_header")
sqlContext.cacheTable("union_header")
dd.show(10000)


dd.write.format('jdbc').options(
    url= url,
    driver=driver,
    dbtable='last_modification',
    user=user,
    password=password).mode('overwrite').save('last_modification')

sqlContext.uncacheTable("union_header")

## -------------------------------------------------------------------


## -------------------------- Performance -------------------------
duration = time() - t0
print("\n\nTotal processing time: %0.2f seconds" %duration)

