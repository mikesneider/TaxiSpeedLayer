from confluent_kafka import Producer
import pandas as pd
import time

pd.options.display.width = 0

class Produce_Data():

    def __init__(self):
        conf = {'bootstrap.servers': 'localhost:9092', 
             'queue.buffering.max.messages': 1000000, 
             'queue.buffering.max.ms' : 500,  ### <==== FIXME: this is fine for large files where the total send time will be above 500ms, but for small files it will add some delay - there is no harm in decreasing this value to something like 10 or 100 ms. On the other hand that may be offset by a lower batch.num.messages, but that is typically not the way to go. 
             'batch.num.messages': 50,   ### <=== FIXME: Why this low value? You typically don't need to alter the batch size.
             'default.topic.config': {'acks': 'all'}}

        self.p = Producer(**conf)
        self.topic = 'nyctrip8'
        self.counter = 0

    def produce_messages(self):
        for chunk in pd.read_csv('/media/sid/0EFA13150EFA1315/NYCTaxiData/trip_data_10.csv',nrows=6000,chunksize=15):
            for i, row in chunk.iterrows():
                self.p.poll(0)
                self.p.produce(self.topic, str(
                    {
                        'medallion': row['medallion'],
                        'pickup_time': int(time.time() - row[' trip_time_in_secs']) * 1000,
                        'dropoff_time': int(time.time()) * 1000,
                        'pickup_loc': {
                            'lat':row[' pickup_latitude'],
                            'lon':row[' pickup_longitude']
                            },
                        'dropoff_loc': {
                            'lat': row[' dropoff_latitude'],
                            'lon': row[' dropoff_longitude']
                            }
                    }
                ).encode('utf-8'))
                print("Produced following row")
                print(row)
                print('----')
                self.counter += 1
                if self.counter == 15:
                    time.sleep(1)
                    self.counter = 0
            self.p.flush()

if __name__ == '__main__':
    kafka_obj = Produce_Data()
    kafka_obj.produce_messages()


