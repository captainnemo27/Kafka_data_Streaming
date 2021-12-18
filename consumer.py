from kafka import KafkaConsumer # pip3 install kafka-python
import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import datetime

TOPIC='covid'

def get_data(message):
   print('reading data ....')
   j_data = json.loads(message)
   print('Time of data: ' + j_data['Date'])
   return pd.json_normalize(j_data['Countries'])

def write_data(data):
   print('importing data ....')
   name_table='covid_19'
   try:
      engine = create_engine('postgresql://admin:123@172.12.0.20:5432/stream_covid',connect_args={'options': '-csearch_path={}'.format('data_covid')})
      data.to_sql(name_table,engine, if_exists='append')
      print("succesffully import to database")
   except:
      print("!!!! Cannot import to database")
   
def transform(data):
      for i in range(data.count()[0]):
        date_time_str = data.at[i,'Date']
        date_time_obj = datetime.strptime(date_time_str ,'%Y-%m-%dT%H:%M:%S.%fZ')
        data.at[i,'Date'] = date_time_obj.date()
        data.at[i,'Time'] = date_time_obj.time()


consumer = KafkaConsumer(TOPIC,
                        bootstrap_servers='127.0.0.1:9092',
                        auto_offset_reset='latest',
                        enable_auto_commit=True
                        )
                        
now_time = None
for msg in consumer:
   print('INFO : Geting new data  ----- Time : ' , datetime.now()  )
   message = msg.value.decode("utf-8")
   j_data = json.loads(message)
   if now_time :
      if now_time < j_data['Date']:
         print('Time : ok')
         data = get_data(message) # convert to dataframe
         transform(data)
         write_data(data)
   else:
         Data = j_data['Date']
         print('Time : ok')
         data = get_data(message) # convert to dataframe
         transform(data)
         write_data(data)
   
   print('INFO : Get data done  ----- Time : ' , datetime.now()  )
import logging
logging.basicConfig(level=logging.INFO)

