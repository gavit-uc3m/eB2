
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
    ta = df_last_mod
    tb = sqlContext.createDataFrame(last_mod_actual, header_schema)

    last_mod_aux = sqlContext.createDataFrame((ta
                    .join(tb, (ta.service == tb.service) & (ta.user == tb.user) & (ta.date == tb.date) & \
                          (ta.data_type == tb.data_type), how='left')
                    .filter(tb.service.isNull())
                    .rdd
                    .map(lambda x: (x[0], x[1], x[2], x[3]))
                   ), header_schema)
    
df_clusters = sqlContext.read.jdbc(url=url, table='clusters', properties=properties)

#Broadcast the date of last modification
broadcast_data_type = sc.broadcast('clustering')
broadcast_date = sc.broadcast(last_mod_actual.collect())
broadcast_clusters = sc.broadcast(df_clusters.rdd.collect())


'''
print('\nbroadcast_date: ')
print(broadcast_date.value)
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

def updateAll(pMicroCluster, oMicroCluster, mc):
    
    for cluster in pMicroCluster:
        
        if (cluster != mc):
            cluster.noNewSamples()
            
    for cluster in oMicroCluster.clusters:
        
        if (cluster != mc):
            cluster.noNewSamples()

def nearestCluster (micro_cluster, sample):
    minDist = 0.0
    minCluster = None
    
    for cluster in micro_cluster:
        
        if (minCluster == None):
            minCluster = cluster
            minDist = np.linalg.norm(sample.value - [cluster[1], cluster[2]])

        dist = np.linalg.norm(sample.value - [cluster[1], cluster[2]])
        dist -= cluster[3]

        if (dist < minDist):
            minDist = dist
            minCluster = cluster
            
    return minCluster



def clustering(x):
    
    from sample import Sample
    from microCluster import MicroCluster
    
    lamb = 0.00001 # The higher the value, the lower importance of the historical data compared to more recent data
    km = 3
    km_per_radian = 6371
    epsilon = km / km_per_radian # Maximum radius of a microcluster
    epsilon = 0.00047
    beta = 0.2 # Minimum density of a microcluster, value between 0 and 1
    mu = 30 # Weigth of a microcluster to form a core-microcluster
    tp = 280*5 #Minimal time period to check if a microcluster is an outlier (5 days in this case)
    minPts = 6
    radiusFactor = 1
    sampleSkip = 3*300 #Three days


    user = x[0]
    data = x[1]
    index = ['Row'+str(i) for i in range(1, len(data)+1)]
    df = pd.DataFrame(data=data, index=index, columns=['time','long','lat']).dropna(axis=1)

    times = df['time']
    buffer_data = df.drop(['time'], axis=1)


    last_mod = [y for y in broadcast_date.value if (y[1] == user) ]
    
    if (len(last_mod) == 0) & (len(data)<sampleSkip):
        
        return

    elif (len(last_mod) == 0) & (len(data)>sampleSkip):

        pMicroCluster = []
        oMicroCluster = []

        currentTimestamp = int(mktime(datetime.date.today().timetuple()))
        
        db = DBSCAN(eps=0.00047, min_samples=minPts, algorithm='brute').fit(buffer_data)
        clusters = db.labels_
        buffer_data['clusters'] = clusters

        clusterNumber = np.unique(clusters)

        for clusterId in clusterNumber:

            if (clusterId != -1):

                cl = buffer_data[buffer_data['clusters'] == clusterId]
                cl = cl.drop('clusters', axis=1)

                mc = MicroCluster(lamb, currentTimestamp)

                for sampleNumber in range(len(cl[1:])):
                    sample = Sample(cl.iloc[sampleNumber].tolist(), currentTimestamp)
                    mc.insertSample(sample, currentTimestamp)

                pMicroCluster.append(mc)

    else:

        pMicroCluster = [MicroCluster(lamb, *y[1:12]) for y in broadcast_clusters.value if (y[0] == user) & (y[-1] == 'pMicroCluster') ]
        oMicroCluster = [MicroCluster(lamb, *y[1:12]) for y in broadcast_clusters.value if (y[0] == user) & (y[-1] == 'oMicroCluster') ]

        for sampleNumber in range(len(buffer_data)):

            sample = Sample(buffer_data.iloc[sampleNumber].values, times.iloc[sampleNumber])
            
            currentTimestamp = int(mktime(datetime.date.today().timetuple()))
            sample.setTimestamp(currentTimestamp)

            merged = False
            TrueOutlier = True

            if len(pMicroCluster) != 0:
                closestMicroCluster = nearestCluster(pMicroCluster, sample)

                backupClosestCluster = copy.deepcopy(closestMicroCluster)
                backupClosestCluster.insertSample(sample, currentTimestamp)

                if (backupClosestCluster.radius <= epsilon):

                    closestMicroCluster.insertSample(sample, currentTimestamp)
                    merged = True
                    TrueOutlier = False

                    updateAll(pMicroCluster, oMicroCluster,closestMicroCluster)

            if not merged and len(oMicroCluster.clusters) != 0:

                closestMicroCluster = nearestCluster(oMicroCluster, sample)

                backupClosestCluster = copy.deepcopy(closestMicroCluster)
                backupClosestCluster.insertSample(sample, currentTimestamp)

                if (backupClosestCluster.radius <= epsilon):
                    closestMicroCluster.insertSample(sample, currentTimestamp)
                    merged = True

                    if (closestMicroCluster.weight > beta * mu):
                        oMicroCluster.clusters.pop(oMicroCluster.clusters.index(closestMicroCluster))
                        pMicroCluster.append(closestMicroCluster)

                    updateAll(pMicroCluster, oMicroCluster, closestMicroCluster)


            if not merged:
                newOutlierMicroCluster = MicroCluster(lamb, currentTimestamp)
                newOutlierMicroCluster.insertSample(sample, currentTimestamp)

                for clusterTest in pMicroCluster.clusters:

                    if np.linalg.norm(clusterTest.center-newOutlierMicroCluster.center) < 2 * epsilon:
                        TrueOutlier = False

                if TrueOutlier:
                    oMicroCluster.append(newOutlierMicroCluster)
                    updateAll(pMicroCluster, oMicroCluster, newOutlierMicroCluster)
                else:
                    pMicroCluster.append(newOutlierMicroCluster)
                    updateAll(pMicroCluster, oMicroCluster, newOutlierMicroCluster)

            if currentTimestamp % tp == 0:

                for cluster in pMicroCluster.clusters:

                    if cluster.weight < beta * mu:
                        pMicroCluster.clusters.pop(pMicroCluster.clusters.index(cluster))

                for cluster in oMicroCluster.clusters:

                    creationTimestamp = cluster.creationTimeStamp

                    xs1 = math.pow(2, -lamb*(currentTimestamp - creationTimestamp + tp)) - 1
                    xs2 = math.pow(2, -lamb * tp) - 1
                    xsi = xs1 / xs2

                    if cluster.weight < xsi:

                        oMicroCluster.clusters.pop(oMicroCluster.clusters.index(cluster))

    output1 = [[y.creationTimeStamp, y.center[0], y.center[1], y.radius, y.weight, y.LS[0], y.LS[1], y.SS[0], y.SS[1], y.N, y.dimensions, 'pMicroCluster'] for y in pMicroCluster]
    output2 = [[y.creationTimeStamp, y.center[0], y.center[1], y.radius, y.weight, y.LS[0], y.LS[1], y.SS[0], y.SS[1], y.N, y.dimensions, 'oMicroCluster'] for y in oMicroCluster]
    return ( user, currentTimestamp, output1, output2 )

def coord_map(x):

    date_all = datetime.datetime.strptime(x['creation_datetime'].split('U',)[0], "%Y-%m-%d %H:%M:%S ")
    day = date_all.date()
    timestamp = int(date_all.strftime("%s"))
    user = x['user']

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

def list_to_array_var(x):
    output_list = np.zeros((1,48))
    for idx, value in enumerate(x[1]):
        output_list[0,int(value[0])] = value[1]
    if len(x[0]) == 4:
        out = ([x[0][3]], output_list.tolist()[0], [x[0][0], x[0][1], x[0][2], 'var'])
    else:
        out = (output_list.tolist()[0], [x[0][0], x[0][1], x[0][2], 'var'])
    return list(y for x in out for y in x)



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
df1 = sqlContext.createDataFrame(last_mod_actual, header_schema)
df2 = sqlContext.createDataFrame(last_mod_rdd, header_schema)
unmodified_df = df1.join(df2, (df1.service == df2.service) & (df1.user == df2.user), how="leftanti")
last_mod = unmodified_df.union(df2)



pCluster_rdd = (data_rdd
                 .map(coord_map)
                 .filter(lambda x: x is not None)
                 .filter(lambda x: x[1][1] != -1.0)
                 .filter(lambda x: x[1][2] != -1.0)
                 .combineByKey(lambda v: [v], lambda x,y: x+[y], lambda x,y: x+y)
                 .map(clustering)
                 .filter(lambda x: x is not None)
                 .filter(lambda x: len(x[2]) > 0)
                 .flatMap(lambda data: [(data[0], data[1], x) for x in data[2]])
                 .map(lambda x: (x[0], x[2][0], float(x[2][1]), float(x[2][2]), float(x[2][3]), float(x[2][4]), float(x[2][5]), \
                    float(x[2][6]), float(x[2][7]), float(x[2][8]), x[2][9], x[2][10], float(0), '', '', '', '', x[2][-1]))
                 )


oCluster_rdd = (data_rdd
                 .map(coord_map)
                 .filter(lambda x: x is not None)
                 .filter(lambda x: x[1][1] != -1.0)
                 .filter(lambda x: x[1][2] != -1.0)
                 .combineByKey(lambda v: [v], lambda x,y: x+[y], lambda x,y: x+y)
                 .map(clustering)
                 .filter(lambda x: x is not None)
                 .filter(lambda x: len(x[3]) > 0)
                 .flatMap(lambda data: [(data[0], data[1], x) for x in data[3]])
                 .map(lambda x: (x[0], x[2][0], float(x[2][1]), float(x[2][2]), float(x[2][3]), float(x[2][4]), float(x[2][5]), \
                    float(x[2][6]), float(x[2][7]), float(x[2][8]), x[2][9], x[2][10], float(0), '', '', '', '', x[2][-1]))
                 )

'''
print("\n\n")
print(pCluster_rdd.take(1))
print("\n")
print(oCluster_rdd.collect())
print("\n")
'''




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

