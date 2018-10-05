
# coding: utf-8

from __future__ import absolute_import
from pyspark.sql import SQLContext, functions, SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from time import time
import datetime
import json
import pprint
import subprocess
import pyspark
import numpy as np
import math
import itertools

print("\n\nInicio codigo\n")
t0 = time() #To measure whole processing time

sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

spark = SparkSession \
    .builder \
    .appName("Spark processing") \
    .getOrCreate()


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
    "driver": driver
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
'''
print('\nbroadcast_date: ')
print(broadcast_date.value)
print('\nlast_mod_aux: ')
print(last_mod_aux.collect())
print('\ndf_last_mod: ')
print(df_last_mod.rdd.collect())
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
where distance is not null
and speed is not null
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
        if (day > last_mod[0][2]) & (day < (datetime.date.today() - datetime.timedelta(days=2))):
            return (service, user, day, broadcast_data_type.value)
        else:
            return
    
def dist_map(x):
    date_all = datetime.datetime.strptime(x['creation_datetime'].split('U',)[0], "%Y-%m-%d %H:%M:%S ")
    day = date_all.date()
    moment = date_all.time()
    slot = math.floor((moment.hour*60+moment.minute+moment.second/60)/30)
    user = x['user']
    value = float(x['distance'])
    service = x['service']
    source = ['source']
    if (source != "garmin") | (source != "fitbit") | (source != "googlefit") :
        source = 'app'
    # The return value is a key-value pair, where the key is (service, user, day, time slot and source) (here only the app)
    return ((service, user, day, slot, source), value)

def speed_map(x):
    date_all = datetime.datetime.strptime(x['creation_datetime'].split('U',)[0], "%Y-%m-%d %H:%M:%S ")
    day = date_all.date()
    moment = date_all.time()
    slot = math.floor((moment.hour*60+moment.minute+moment.second/60)/30)
    user = x['user']
    value = float(x['speed'])
    service = x['service']
    return ((service, user, day, slot), value)

def coordinate_map(x):
    date_all = datetime.datetime.strptime(x['creation_datetime'].split('U',)[0], "%Y-%m-%d %H:%M:%S ")
    day = date_all.date()
    moment = date_all.time()
    slot = math.floor((moment.hour*60+moment.minute+moment.second/60)/30)
    user = x['user']
    latitude = float(x['longitude'])
    longitude = float(x['latitude'])
    return ((user, day, 'app', slot), (latitude,longitude))

def at_home(x):
    a = (sin(radians(x[1][0])/2))**2 + cos(radians(x[1][0])) * (sin(radians(x[1][1])/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = 6373.0 * c   #está en km
   
    if (x[0][1] < (date.today() - timedelta(days=4))):
        #está en casa si la distancia al 0 es menor de 500m
        return(x[0],(1 if distance <= 0.5 else 0))
    else:
        return

def filter_data(x):
    last_mod = [y for y in broadcast_date.value if (y[1] == x[0][1]) ]
        
    if len(last_mod) == 0:
        if (x[0][2] < (datetime.date.today() - datetime.timedelta(days=2))):
            return x
        else:
            return
    else:
        if ((x[0][2] > last_mod[0][2]) & (x[0][2] < (datetime.date.today() - datetime.timedelta(days=2)))):
            return x
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

def list_to_array_home(x):
    output_list = np.zeros((1,48))
    for idx, value in enumerate(x[1]):
        output_list[0,value[0]] = value[1]
    out = ([x[0][2]], output_list.tolist()[0], [broadcast_service.value , x[0][0], x[0][1], 'home'])
   
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


dist_mean = (data_rdd
             .map(dist_map)
             .map(filter_data)
             .filter(lambda x: x is not None)
             .aggregateByKey(0,lambda acc,val: acc+val, lambda acc1, acc2: acc1+acc2)
             .map(lambda x: ( (x[0][0], x[0][1], x[0][2], x[0][4]), (x[0][3], x[1]) ))
             .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
             .map(list_to_array)
            )

speed_mean = (data_rdd
              .map(speed_map)
              .map(filter_data)
              .filter(lambda x: x is not None)
              .aggregateByKey( (0,0), lambda a,b: (a[0]+b, a[1]+1), lambda a,b: (a[0]+b[0], a[1]+b[1]) )
              .mapValues(lambda v: v[0]/v[1])
              .map(lambda x: ( (x[0][0], x[0][1], x[0][2]), (x[0][3], x[1]) ))
              .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
              .map(list_to_array)
             )

dist_var = (data_rdd
            .map(dist_map)
            .map(filter_data)
            .filter(lambda x: x is not None)
            .mapValues(lambda x: (1, x, x*x))
            .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
            .mapValues(lambda x: (x[2]/x[0] - (x[1]/x[0])**2))
            .map(lambda x: ( (x[0][0], x[0][1], x[0][2], x[0][4]), (x[0][3], x[1]) ))
            .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
            .map(list_to_array_var)
           )

speed_var = (data_rdd
             .map(speed_map)
             .map(filter_data)
             .filter(lambda x: x is not None)
             .mapValues(lambda x: (1, x, x*x))
             .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
             .mapValues(lambda x: (x[2]/x[0] - (x[1]/x[0])**2))
             .map(lambda x: ( (x[0][0], x[0][1], x[0][2]), (x[0][3], x[1]) ))
             .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
             .map(list_to_array_var)
           )

dist_home = (datapoint_rdd
               .map(coordinate_map)
               .map(at_home)
               .filter(lambda x: x is not None)
               .groupByKey()
               .map(lambda x: (x[0], list(x[1])))
               .map(lambda x: ((x[0][0],x[0][1], x[0][2]), (x[0][3], max(x[1], key = x[1].count))))
               .groupByKey()
               .map(lambda x: (x[0], list(x[1])))
               .map(list_to_array_home)
             )


print("\n\n")
#print(data_rdd.count())
#print(len(broadcast_date.value))
#print(broadcast_date.value)
#print(header.take(1))
#print(header.count())
#print(dist_mean.take(1))
#print(speed_mean.take(1))
#print(dist_var.take(1))
#print(speed_var.take(1))
#last_mod.union(last_mod_aux).show()

## -------------------------------------------------------------------



## ------------------- Saving to MySQL database ----------------------

# WRITE IN "mobility_summary" TABLE


mob_sum_schema = StructType([StructField('service', StringType(), False),
                               StructField('user', StringType(), False),
                               StructField('date', DateType(), False)
                              ])

# To avoid duplicates in father table if code is not executed till the end
mob_sum_new = sqlContext.createDataFrame(header.map(lambda a: (a[0], a[1], a[2])), mob_sum_schema)
mob_sum_old = sqlContext.read.jdbc(url=url, table="mobility_summary", properties=properties)
mob_sum_df = mob_sum_new.join(mob_sum_old, (mob_sum_new.service == mob_sum_old.service) & (mob_sum_new.user == mob_sum_old.user) & (mob_sum_new.date == mob_sum_old.date), how="leftanti")

mob_sum_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="mobility_summary",
      user=user,
      password=password).mode('append').save()

#mob_sum_df.show(1)


## WRITE IN "distance" TABLE

dist_schema = StructType([StructField('source', StringType(), False),
                          StructField('Slot1', FloatType(), False),
                          StructField('Slot2', FloatType(), False),
                          StructField('Slot3', FloatType(), False),
                          StructField('Slot4', FloatType(), False),
                          StructField('Slot5', FloatType(), False),
                          StructField('Slot6', FloatType(), False),
                          StructField('Slot7', FloatType(), False),
                          StructField('Slot8', FloatType(), False),
                          StructField('Slot9', FloatType(), False),
                          StructField('Slot10', FloatType(), False),
                          StructField('Slot11', FloatType(), False),
                          StructField('Slot12', FloatType(), False),
                          StructField('Slot13', FloatType(), False),
                          StructField('Slot14', FloatType(), False),
                          StructField('Slot15', FloatType(), False),
                          StructField('Slot16', FloatType(), False),
                          StructField('Slot17', FloatType(), False),
                          StructField('Slot18', FloatType(), False),
                          StructField('Slot19', FloatType(), False),
                          StructField('Slot20', FloatType(), False),
                          StructField('Slot21', FloatType(), False),
                          StructField('Slot22', FloatType(), False),
                          StructField('Slot23', FloatType(), False),
                          StructField('Slot24', FloatType(), False),
                          StructField('Slot25', FloatType(), False),
                          StructField('Slot26', FloatType(), False),
                          StructField('Slot27', FloatType(), False),
                          StructField('Slot28', FloatType(), False),
                          StructField('Slot29', FloatType(), False),
                          StructField('Slot30', FloatType(), False),
                          StructField('Slot31', FloatType(), False),
                          StructField('Slot32', FloatType(), False),
                          StructField('Slot33', FloatType(), False),
                          StructField('Slot34', FloatType(), False),
                          StructField('Slot35', FloatType(), False),
                          StructField('Slot36', FloatType(), False),
                          StructField('Slot37', FloatType(), False),
                          StructField('Slot38', FloatType(), False),
                          StructField('Slot39', FloatType(), False),
                          StructField('Slot40', FloatType(), False),
                          StructField('Slot41', FloatType(), False),
                          StructField('Slot42', FloatType(), False),
                          StructField('Slot43', FloatType(), False),
                          StructField('Slot44', FloatType(), False),
                          StructField('Slot45', FloatType(), False),
                          StructField('Slot46', FloatType(), False),
                          StructField('Slot47', FloatType(), False),
                          StructField('Slot48', FloatType(), False),
                           StructField('mobility_summary_service', StringType(), False),
                           StructField('mobility_summary_user', StringType(), False),
                           StructField('mobility_summary_date', DateType(), False),
                           StructField('type', StringType(), False)
                         ])

dist_mean_df = sqlContext.createDataFrame(dist_mean, dist_schema)
dist_var_df = sqlContext.createDataFrame(dist_var,  dist_schema)

dist_mean_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="distance",
      user=user,
      password=password).mode('append').save()

dist_var_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="distance",
      user=user,
      password=password).mode('append').save()

#dist_mean_df.show(1)



## WRITE IN "speed" TABLE

speed_schema = StructType([StructField('Slot1', FloatType(), False),
                          StructField('Slot2', FloatType(), False),
                          StructField('Slot3', FloatType(), False),
                          StructField('Slot4', FloatType(), False),
                          StructField('Slot5', FloatType(), False),
                          StructField('Slot6', FloatType(), False),
                          StructField('Slot7', FloatType(), False),
                          StructField('Slot8', FloatType(), False),
                          StructField('Slot9', FloatType(), False),
                          StructField('Slot10', FloatType(), False),
                          StructField('Slot11', FloatType(), False),
                          StructField('Slot12', FloatType(), False),
                          StructField('Slot13', FloatType(), False),
                          StructField('Slot14', FloatType(), False),
                          StructField('Slot15', FloatType(), False),
                          StructField('Slot16', FloatType(), False),
                          StructField('Slot17', FloatType(), False),
                          StructField('Slot18', FloatType(), False),
                          StructField('Slot19', FloatType(), False),
                          StructField('Slot20', FloatType(), False),
                          StructField('Slot21', FloatType(), False),
                          StructField('Slot22', FloatType(), False),
                          StructField('Slot23', FloatType(), False),
                          StructField('Slot24', FloatType(), False),
                          StructField('Slot25', FloatType(), False),
                          StructField('Slot26', FloatType(), False),
                          StructField('Slot27', FloatType(), False),
                          StructField('Slot28', FloatType(), False),
                          StructField('Slot29', FloatType(), False),
                          StructField('Slot30', FloatType(), False),
                          StructField('Slot31', FloatType(), False),
                          StructField('Slot32', FloatType(), False),
                          StructField('Slot33', FloatType(), False),
                          StructField('Slot34', FloatType(), False),
                          StructField('Slot35', FloatType(), False),
                          StructField('Slot36', FloatType(), False),
                          StructField('Slot37', FloatType(), False),
                          StructField('Slot38', FloatType(), False),
                          StructField('Slot39', FloatType(), False),
                          StructField('Slot40', FloatType(), False),
                          StructField('Slot41', FloatType(), False),
                          StructField('Slot42', FloatType(), False),
                          StructField('Slot43', FloatType(), False),
                          StructField('Slot44', FloatType(), False),
                          StructField('Slot45', FloatType(), False),
                          StructField('Slot46', FloatType(), False),
                          StructField('Slot47', FloatType(), False),
                          StructField('Slot48', FloatType(), False),
                           StructField('mobility_summary_service', StringType(), False),
                           StructField('mobility_summary_user', StringType(), False),
                           StructField('mobility_summary_date', DateType(), False),
                           StructField('type', StringType(), False)
                         ])

speed_mean_df = sqlContext.createDataFrame(speed_mean, speed_schema)
speed_var_df = sqlContext.createDataFrame(speed_var,  speed_schema)

speed_mean_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="speed",
      user=user,
      password=password).mode('append').save()

speed_mean_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="speed",
      user=user,
      password=password).mode('append').save()

#speed_mean_df.show(1)


## WRITE IN "distance" TABLE for "at_home" data


dist_home_df = sqlContext.createDataFrame(dist_home,  dist_schema)

dist_home_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="distance",
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



## ----- Performance
duration = time() - t0
num_data = dist_mean.count()
print("\n\nTotal processing time: %0.2f seconds" %duration)
print("Total processed data: %i users*days" %num_data)
print("Performance: %0.2f seconds/(user*day)\n\n" %(float(duration)/(num_data+np.finfo(float).eps)))


