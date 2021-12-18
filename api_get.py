import requests
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)
from kafka import KafkaProducer

url = "https://api.covid19api.com/summary"
payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
# j_data=response.json()
# data = pd.json_normalize(j_data['Countries'])

# producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
# TOPIC = 'covid'
message = response.text
# message = bytearray(message.encode("utf-8"))
# producer.send(TOPIC, message).get(timeout=30)
print(message)
