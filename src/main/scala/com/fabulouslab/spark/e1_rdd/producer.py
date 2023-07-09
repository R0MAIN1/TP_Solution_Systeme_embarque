import random
import time
from datetime import datetime
from kafka import KafkaProducer
import json

bootstrap_servers = '127.0.0.1:9092'
topic = 'new_topic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

users = ['user_1', 'user_2', 'user_3', 'user_4']
pages = ['page_1', 'page_2', 'page_3', 'page_4']

def generate_data():
    user = random.choice(users)
    page = random.choice(pages)
    milliseconds = random.randint(0, 180000)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    data = {
        'Id_user': user,
        'Id_page': page,
        'milliseconde': milliseconds,
        'date': timestamp
    }
    return data

def send_data(data):
    producer.send(topic, value=data)

def add_datatokafka():
    while True:
        data = generate_data()
        send_data(data)
        time.sleep(1)

if __name__ == "__main__":
    add_datatokafka()