from confluent_kafka import Consumer
import pandas as pd
from ast import literal_eval
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time

pd.set_option('display.float_format', lambda x: '%.3f' % x)

class Consume_Data():

    def __init__(self):
        self.topic = 'nycspeed12'
        self.df = pd.DataFrame()

    def ConsumeMessages(self):
        c = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'mygroup1',
            'auto.offset.reset': 'earliest'
        })

        c.subscribe([self.topic])
        self.counter = 0

        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            self.data = literal_eval(msg.value().decode('utf-8'))
            # print(self.data)
            self.df = self.df.append(self.data,ignore_index=True)
            self.counter += 1
            # print(self.df)
            self.es = Elasticsearch()
            self.data = []
            if self.counter >= 15:
                for i, row in self.df.iterrows():
                    # print(row)
                    self.data.append({
                        "_index": 'nyctrip8',
                        "type": 'geodata',
                        "_source": {
                            'medallion': row['medallion'],
                            'pickup_time': row['pickup_time'],
                            'dropoff_time': row['dropoff_time'],
                            'pickup_loc': {
                                'lat': row['pickup_loc']['lat'],
                                'lon': row['pickup_loc']['lon']
                            },
                            'dropoff_loc': {
                                'lat': row['dropoff_loc']['lat'],
                                'lon': row['dropoff_loc']['lon']
                            }
                        }
                    })
                print(self.data)
                helpers.bulk(self.es, self.data)
                print("data inserted")
                self.counter = 0

        c.close()

if __name__ == '__main__':
    consumer_obj = Consume_Data()
    consumer_obj.ConsumeMessages()

