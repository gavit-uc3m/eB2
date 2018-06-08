from datetime import datetime
from eb2_json import get_datapoints

db_name = 'gts-node'
users = [['test-lg-g4']]
#[['pruebabqe5s', 'pruebas7', 'bqabtest', 'pruebabqsara', 'pruebamotog4plus', 'acobo-das-firebase']] 
#[['test-lg-g4']]  # utilizar '' para todos los usuarios de la base de datos

start = datetime(2017,12,1).strftime('%Y-%m-%d')
end = datetime(2017,12,10).strftime('%Y-%m-%d')

time_period = {
    "start" : start,
    "end" : end
}


data_type= "geowithplaces"
#data_type= "wifis"

# Automaticamente genera el .m
for user in users:
    #for data_type in data_types:
        get_datapoints(
            db_name=db_name,
            users=user,
            time_period=time_period,
            data_type=data_type
        )
