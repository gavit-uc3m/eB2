

from __future__ import absolute_import
from pyspark.sql import SQLContext, functions, SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from time import time
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, upper,udf
from pyspark.sql import Row
from datetime import date, timedelta   
import datetime as dt
from operator import itemgetter
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
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_inputa'.format(bucket)
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
                   .filter(lambda x: x[-1] == 'test')
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
broadcast_data_type = sc.broadcast('test')
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


sql_calls = "SELECT * from data where type = 'callreg' "
calls_type = sqlContext.sql(sql_calls)
data_calls= calls_type.rdd


# COMPUTE calls
def call_map(x):
    date_all = dt.datetime.strptime(x['date_time'].split(' U',)[0].split('.',)[0], "%Y-%m-%d %H:%M:%S")

    service = x['service']
    user = x['user']
    day = date_all.date()
    duration = int(x['duration'])
    number = x['destination_number']
    call_type = x['call_type']

    last_mod = [y for y in broadcast_date.value if (y[1] == user) ]
        
    if len(last_mod) == 0:
        if (day < (date.today() - timedelta(days=2))):
            return ((service, user, day) , (duration, number, call_type))
        else:
            return
    else:
        if ((day > last_mod[0][2]) & (day < (date.today() - timedelta(days=2)))):
            return ((service, user, day) , (duration, number, call_type))
        else:
            return

def compute_outgoing(x):

    outgoing = 0
    for call in x[1]:
      if call[-1] == 'OUTGOING':
        outgoing += 1

    out_percent = int(outgoing*100//len(x[1]))

    return (x[0], out_percent)

def weekly_group(x):
    service = x[0][0]
    user = x[0][1]
    day = x[0][2]
    val = x[1]

    return ((service, user, day.year, day.isocalendar()[1]), val )



outgoing_calls = (data_calls
  .map(call_map)
  .filter(lambda x: x is not None)
  .map(weekly_group)
  .combineByKey(lambda v:[v],lambda x,y:x+[y],lambda x,y:x+y)
  .map(compute_outgoing)
  .map(lambda x: (x[0][0], x[0][1], dt.datetime.strptime(str(x[0][2])+ '-' + str(x[0][3]) + '-1', "%Y-%W-%w").date(), x[1]) )
  )

contact_entr = (data_calls
  .map(call_map)
  .filter(lambda x: x is not None)
  .filter(lambda x: x[0][-1] > dt.date(2018,10,1))
  .map(lambda x: (x[0], (x[1][1]) ) )
  .map(weekly_group)
  .distinct()
  .combineByKey(lambda v:[v],lambda x,y:x+[y],lambda x,y:x+y)
  #.filter(lambda x: len(x[1]) > 0)
  .map(lambda x: (x[0], math.log(len(x[1]), 2)) ) 
  .map(lambda x: (x[0][0], x[0][1], dt.datetime.strptime(str(x[0][2])+ '-' + str(x[0][3]) + '-1', "%Y-%W-%w").date(), x[1]) )
  )


#print('\nOutgoing:')
#print(outgoing_calls.take(10))
#print('\nEntropy:')
#print(contact_entr.take(10))


exit()




#group activity by day and slot
def app_flat(x):
    s = x[1]
    app_list = []
    for i in range(len(s)):
        for j in range(len(s[i])):
            app_list.append([s[i][j]['name'],s[i][j]['duration']])
   
    return(x[0],app_list)

#sum total time of each app in the slot
def app_sum(x):
    s = x[1]
    app_grouped = []
    r = 1
    app_grouped.append(s[0])
    for i in range(len(s)-1):
        r = len(app_grouped)
        esta = False
        for j in range(r):
            if s[i+1][0] == app_grouped[j][0]:
                app_grouped[j][1] = int(app_grouped[j][1]) + int(s[i+1][1])
                #app_grouped[j][1] = str(app_grouped[j][1])
                esta = True
                break            
        if esta == False:
            app_grouped.append(s[i+1])
        
    return(x[0],app_grouped)

#pasamos de milisegundos a segundos
def convert_seconds(x):
    s = x[1]
    result = []
    for i in range(len(s)): 
        s[i][1] = math.floor(int(s[i][1])/1000)
        result.append([s[i][0],int(s[i][1])])
   
    return(x[0],result)

#quitamos los de launcher y los que se usan menos de 1 segundo
def filter_launcher(x):
    s = x[1]
    result = []
    for i in range(len(s)): 
        if not 'launcher' in s[i][0]:
            if s[i][1] != 0:
                result.append(s[i])
   
    return(x[0],result)

#agrupamos en array el nombre y el tiempo para luego pasarlo a dataframe
def forma_df(x):
    s = x[1]
    nombre = []
    tiempo = []
    for i in range(len(s)):
        nombre.append(s[i][0])
        tiempo.append(s[i][1])
        
    return (x[0],nombre, tiempo)


#call list grouped by user and by day
calls_day_list = (calls
                  .groupByKey()
                  .map(lambda x: (x[0],list(x[1])))
                  )

#print(calls_day_list.take(10))

app_forma_df = (appusage
                .groupByKey()
                .map(lambda x: (x[0], list(x[1])))
                .map(app_flat)
                .map(app_sum)
                .map(convert_seconds)
                .map(filter_launcher)
                .map(forma_df)
               )
#juntamos los dos dataframes para tener todos los usuarios y fechas en cuenta

prueba_apps = (app_forma_df.map(lambda x: ((x[0][0],x[0][1], x[0][2]),[x[0][3],x[1],x[2]]))
               .groupByKey()
               .map(lambda x: (x[0],list(x[1]))) 
              )

junto = calls_day_list.fullOuterJoin(prueba_apps)
print(junto.count())

junto2 = prueba_apps.fullOuterJoin(calls_day_list)
print(junto2.count())



social_resumen = junto.map(lambda x: (x[0][0], x[0][1], x[0][2], broadcast_data_type.value)).filter(lambda x: x is not None).distinct()

if social_resumen.count() == 0:
    last_mod_rdd = last_mod_actual
    print('Header vacio')
else:
    last_mod_rdd = (social_resumen
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
#last_mod.show()


## ------------------- Saving to MySQL database ----------------------

# WRITE IN "mobility_summary" TABLE


schema_resumen = StructType([StructField('service', StringType(), False),
                     StructField('user', StringType(),False),
                     StructField('date', DateType(),False)                   
                    ])

# To avoid duplicates in father table if code is not executed till the end

summary_new = sqlContext.createDataFrame(social_resumen.map(lambda a: (a[0], a[1], a[2])),schema_resumen)
summary_old = sqlContext.read.jdbc(url=url, table="social_summary", properties=properties)
summary_df = summary_new.join(summary_old, (summary_new.service == summary_old.service) & \
            (summary_new.user == summary_old.user) & (summary_new.date == summary_old.date), \
            how="leftanti")

summary_df.show()


summary_df.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="social_summary",
      user=user,
      password=password).mode('append').save()



schema_apps = StructType([StructField('slot', IntegerType()),
                          
                           StructField('app', ArrayType(StringType(), True)),
                           StructField('time', ArrayType(IntegerType(), True)),
                           
                           StructField('social_summary_service', StringType()),
                           StructField('social_summary_user', StringType()),
                           StructField('social_summary_date', DateType())
                          ])

df_final_apps = sqlContext.createDataFrame(app_forma_df.map(lambda x: [x[0][3],x[1],x[2],x[0][0],x[0][1],x[0][2]]),schema_apps)
#df_final_apps.printSchema()

def dualExplode(r):
    rowDict = r.asDict()
    bList = rowDict.pop('app')
    cList = rowDict.pop('time')
    for app,time in zip(bList, cList):
        newDict = dict(rowDict)
        newDict['app'] = app
        newDict['time'] = time
        yield Row(**newDict)

schema_2 = StructType([StructField('app', StringType()),
                       StructField('slot', IntegerType()),
                       StructField('social_summary_service', StringType()),
                       StructField('social_summary_user', StringType()),
                       StructField('social_summary_date', DateType()),
                       StructField('time', IntegerType())
                      ])        
        
df_split = sqlContext.createDataFrame(df_final_apps.rdd.flatMap(dualExplode)).select(col('slot').cast(IntegerType()), 'app', col('time').cast(IntegerType()),'social_summary_service','social_summary_user','social_summary_date')
df_split.show()


df_split.write.format('jdbc').options(
      url= url,
      driver=driver,
      dbtable="appusage",
      user=user,
      password=password).mode('append').save()



schema_calls = StructType([StructField('time', IntegerType()),
                           StructField('duration', IntegerType()),
                           StructField('number', StringType()),
                           StructField('type', StringType()),
                           StructField('social_summary_service', StringType()),
                           StructField('social_summary_user', StringType()),
                           StructField('social_summary_date', DateType())
                          ])

df_final_calls = sqlContext.createDataFrame(calls.map(lambda x: [x[1][0], x[1][1], x[1][2], x[1][3], x[0][0], x[0][1], x[0][2]]),schema_calls)
df_final_calls.show()


df_final_calls.write.format('jdbc').options(
     url= url,
     driver=driver,
     dbtable="call_register",
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

