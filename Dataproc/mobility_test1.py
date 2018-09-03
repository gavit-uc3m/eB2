
# coding: utf-8

from __future__ import absolute_import
from pyspark.sql import SQLContext, functions
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from time import time
#from datetime import datetime, timedelta, date
import datetime
import datetime
import json
import pprint
import subprocess
import pyspark
import numpy as np
import math
import itertools

print("\n\nInicio codigo\n")


sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)


bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input'.format(bucket)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)


'''
## ---------- Reading from last_modification table -------------------

url = "jdbc:mysql://35.195.77.246/test-node"
user = "root"
password = "root"
driver = "com.mysql.jdbc.Driver"
dbtable = 'last_modification'
properties = {
    "user": user,
    "password": password,
    "driver": driver
    }

header_schema = StructType([StructField('service', StringType(), True),
                StructField('user', StringType(), True),
                StructField('date', DateType(), True),
                StructField('data_type', StringType(), False)
                ])

df_last_mod = sqlContext.read.jdbc(url=url, table=dbtable, properties=properties)


#Filter the service and the type of data to be procesed
last_mod_actual = (df_last_mod
                   .rdd
                   .filter(lambda x: x[-1] == 'mobility')
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
    

#Broadcast the date of last modification
broadcast_data_type = sc.broadcast('mobility')
broadcast_date = sc.broadcast(last_mod_actual.collect())

print('\nbroadcast_date: ')
print(broadcast_date.value)
print('\nlast_mod_aux: ')
print(last_mod_aux.collect())
print('\ndf_last_mod: ')
print(df_last_mod.rdd.collect())

## -------------------------------------------------------------------
'''


## --------------------- Reading from BigQuery ------------------------
t0 = time() #To measure time

conf = {
    # Input Parameters.
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': project,
    'mapred.bq.input.dataset.id': 'Testbigquery',
    'mapred.bq.input.table.id': 'Datapoints',
}

# Load data in from BigQuery.
data_rdd = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf).map(lambda x: json.loads(x[1]))


duration = time() - t0
print("Table correctly read in %0.2f seconds\n" %duration)
## -------------------------------------------------------------------


print("\ncreation_datetime\n")
print(data_rdd.map(lambda x: datetime.datetime.strptime(x['creation_datetime'].split('U',)[0], "%Y-%m-%d %H:%M:%S ")).take(1))
print("\n\n")
