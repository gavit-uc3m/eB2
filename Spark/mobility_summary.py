
# coding: utf-8
# Code by: Gaspar Avit, 7/6/2018


from pyspark.sql import SQLContext, functions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from time import time
from datetime import datetime, timedelta, date

import numpy as np
import math
import itertools


sc = SparkContext(conf=SparkConf().setAppName(“Spark processing“)) 
sqlContext = SQLContext(sc)
url = "jdbc:mysql://eb2-01/testdb"
user = "dbadmin"
password = ".fG}zbF]TZMUu?TC2Q9E"
driver = "com.mysql.jdbc.Driver"
services = ['sleep-node', 'afsp', 'gts-node', 'test-node', 'das-node']
data_type = 'mobility'
dbtable = 'last_modification'

for i in range(0,len(services)):

#Defining header schema
header_schema = StructType([StructField('service', StringType(), True),
                StructField('user', StringType(), True),
                StructField('date', DateType(), True),
                StructField('data_type', StringType(), False)
                ])

#Reading from last_modification table
properties = {
    "user": user,
    "password": password,
    "driver": driver,
    }

df_last_mod = sqlContext.read.jdbc(url=url, table="last_modification", properties=properties)


#Filter the service and the type of data to be procesed
last_mod_actual = (df_last_mod
                   .rdd
                   .filter(lambda x: x[0] == service)
                   .filter(lambda x: x[-1] == data_type)
                  )

#Take the data that is not going to be procesed
if last_mod_actual.count() == 0:
    last_mod_aux = df_last_mod.rdd
else:
    ta = df_last_mod
    tb = sqlContext.createDataFrame(last_mod_actual, header_schema)

    last_mod_aux = (ta
                    .join(tb, (ta.service == tb.service) & (ta.user == tb.user) & (ta.date == tb.date) & \
                          (ta.data_type == tb.data_type), how='left')
                    .filter(tb.service.isNull())
                    .rdd
                    .map(lambda x: (x[0], x[1], x[2], x[3]))
                   )
    

#Broadcast the date of last modification
broadcast_service = sc.broadcast(service)
broadcast_data_type = sc.broadcast(data_type)
broadcast_date = sc.broadcast(last_mod_actual.collect())



# # Reading from mongoDB

# Define Datapoint Schema
attributesSchema = StructType([StructField('distance', StringType(), False),
                               StructField('speed', StringType(), False)
                            ])

coordinatesSchema = StructType([StructField('altitude', StringType(), False),
                                StructField('accuracy', StringType(), False)
                               ])

bodySchema = StructType([StructField('attributes', attributesSchema, True),
                         #StructField('effective_time_frame', time_frameSchema, True),
                         StructField('coordinates', coordinatesSchema, True)
                        ])

headerSchema = StructType([StructField('user',StringType(), False),
                           StructField('service',StringType(), False),
                           #StructField('id', StringType(),False),
                           StructField('creation_date_time', StringType(),False),
                           StructField('acquisition_provenance', StringType(),False),
                           #StructField('schema_id', StringType(),False)
                            ])

datapointSchema = StructType([StructField('header', headerSchema, False),
                              StructField('body', bodySchema, False)
                            ])

# Read Data from mongoDB
uri = 'mongodb://eb2-01:27017/'+service+'.datapoints'
df_app =(sqlContext
     .read
     .format('com.mongodb.spark.sql.DefaultSource')
     .option('spark.mongodb.input.uri', uri)
     .option("inferSchema", "false")
     .schema(datapointSchema)
     .load()
    )

# Register a temp table
df_app.registerTempTable("temp_table")
datapoint_table = sqlContext.table("temp_table")



datapoint_rdd = (datapoint_table
                 .rdd
                 .filter(lambda x: x.body.attributes is not None)
                 .filter(lambda x: float(x.body.attributes.distance) > 0)
                 .filter(lambda x: float(x.body.attributes.speed) > 0)
                 .filter(lambda x: float(x.body.coordinates.accuracy) < 200)
                )



def header_map(x):
    day = datetime.fromtimestamp(int(x.header.creation_date_time[12:-2])/1000).date()
    user = x.header.user
    
    last_mod = [y for y in broadcast_date.value if (y[1] == user)]

    if len(last_mod) == 0:
        if day < (date.today() - timedelta(days=2)):
            return (broadcast_service.value, user, day, broadcast_data_type.value)
    else:
        if (day > last_mod[0][2]) & (day < (date.today() - timedelta(days=2))):
            return (broadcast_service.value, user, day, broadcast_data_type.value)
        else:
            return
    
def dist_map(x):
    day = datetime.fromtimestamp(int(x.header.creation_date_time[12:-2])/1000).date()
    moment = datetime.fromtimestamp(int(x.header.creation_date_time[12:-2])/1000).time()
    slot = math.floor((moment.hour*60+moment.minute+moment.second/60)/30)
    user = x.header.user
    value = float(x.body.attributes.distance)
    # The return value is a key-value pair, where the key is user, day, time slot and source (here only the app)
    return ((user, day, slot, 'app'), value)

def speed_map(x):
    day = datetime.fromtimestamp(int(x.header.creation_date_time[12:-2])/1000).date()
    moment = datetime.fromtimestamp(int(x.header.creation_date_time[12:-2])/1000).time()
    slot = math.floor((moment.hour*60+moment.minute+moment.second/60)/30)
    user = x.header.user
    value = float(x.body.attributes.speed)
    return ((user, day, slot), value)

def filter_data(x):
    last_mod = [y for y in broadcast_date.value if (y[1] == x[0][0]) ]
        
    if len(last_mod) == 0:
        if x[0][1] < (date.today() - timedelta(days=2)):
            return x
    else:
        if (x[0][1] > last_mod[0][2]) & (x[0][1] < (date.today() - timedelta(days=2))):
            return x
        else:
            return

def list_to_array(x):
    output_list = -1*(np.ones((1,48)))
    for idx, value in enumerate(x[1]):
        if value[1] > 0.0:
            output_list[0,value[0]] = value[1]
    if len(x[0]) == 3:
        out = ([x[0][2]], output_list.tolist()[0], [broadcast_service.value, x[0][0], x[0][1], 'mean'])
    else:
        out = (output_list.tolist()[0], [broadcast_service.value, x[0][0], x[0][1], 'mean'])
    return list(y for x in out for y in x)

def list_to_array_var(x):
    output_list = np.zeros((1,48))
    for idx, value in enumerate(x[1]):
        output_list[0,value[0]] = value[1]
    if len(x[0]) == 3:
        out = ([x[0][2]], output_list.tolist()[0], [broadcast_service.value, x[0][0], x[0][1], 'var'])
    else:
        out = (output_list.tolist()[0], [broadcast_service.value, x[0][0], x[0][1], 'var'])
    return list(y for x in out for y in x)



header = (datapoint_rdd
          .map(header_map)
          .filter(lambda x: x is not None)
          .distinct()
         )

if header.count() == 0:
    last_mod_rdd = last_mod_actual
else:
    last_mod_rdd = (header
                    .map(lambda x: ((x[0], x[1], x[3]), x[2]))
                    .groupByKey()
                    .map(lambda x: (x[0], sorted(list(x[1]))))
                    .map(lambda x: (x[0], x[1][-1]))
                    .map(lambda x: (x[0][0], x[0][1], x[1], x[0][2]))
                   )

dist_mean = (datapoint_rdd
             .map(dist_map)
             .map(filter_data)
             .filter(lambda x: x is not None)
             .aggregateByKey(0,lambda acc,val: acc+val, lambda acc1, acc2: acc1+acc2)
             .map(lambda x: ( (x[0][0], x[0][1], x[0][3]), (x[0][2], x[1]) ))
             .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
             .map(list_to_array)
            )

speed_mean = (datapoint_rdd
              .map(speed_map)
              .map(filter_data)
              .filter(lambda x: x is not None)
              .aggregateByKey( (0,0), lambda a,b: (a[0]+b, a[1]+1), lambda a,b: (a[0]+b[0], a[1]+b[1]) )
              .mapValues(lambda v: v[0]/v[1])
              .map(lambda x: ( (x[0][0], x[0][1]), (x[0][2], x[1]) ))
              .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
              .map(list_to_array)
             )

dist_var = (datapoint_rdd
            .map(dist_map)
            .map(filter_data)
            .filter(lambda x: x is not None)
            .mapValues(lambda x: (1, x, x*x))
            .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
            .mapValues(lambda x: (x[2]/x[0] - (x[1]/x[0])**2))
            .map(lambda x: ( (x[0][0], x[0][1], x[0][3]), (x[0][2], x[1]) ))
            .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
            .map(list_to_array_var)
           )

speed_var = (datapoint_rdd
             .map(speed_map)
             .map(filter_data)
             .filter(lambda x: x is not None)
             .mapValues(lambda x: (1, x, x*x))
             .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
             .mapValues(lambda x: (x[2]/x[0] - (x[1]/x[0])**2))
             .map(lambda x: ( (x[0][0], x[0][1]), (x[0][2], x[1]) ))
             .combineByKey(lambda v:[(v[0], v[1])],lambda x,y:x+[y],lambda x,y:x+y)
             .map(list_to_array_var)
           )

# # Save to MySQL database


## WRITE IN "mobility_summary" TABLE

mob_sum_schema = StructType([StructField('service', StringType(), False),
                               StructField('user', StringType(), False),
                               StructField('date', DateType(), False)
                              ])

mob_sum_df = sqlContext.createDataFrame(header.map(lambda a: (a[0], a[1], a[2])), mob_sum_schema)

mob_sum_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="mobility_summary",
      user=user,
      password=password).mode('append').save()



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


# Update last modification

header_schema = StructType([StructField('service', StringType(), False),
                               StructField('user', StringType(), False),
                               StructField('date', DateType(), False),
                               StructField('data_type', StringType(), False)
                              ])

union_header = (sqlContext
                .createDataFrame(last_mod_rdd.union(last_mod_aux), header_schema)
                .registerTempTable("union_header")
               )

dd = sqlContext.table("union_header")
sqlContext.cacheTable("union_header")
dd.show((last_mod_rdd.union(last_mod_aux)).count()+100)

dd.write.format('jdbc').options(
    url= url,
    driver=driver,
    dbtable=dbtable,
    user=user,
    password=password).mode('overwrite').save(dbtable)

sqlContext.uncacheTable("union_header")

