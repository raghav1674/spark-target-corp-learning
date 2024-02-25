from confluent_kafka import Producer
import socket
import pandas as pd
import json

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

src_file = "../data/target_data.csv"
topic = "assignment"

conf = {'bootstrap.servers': '192.168.56.131:9092',
      'client.id': socket.gethostname()}

producer = Producer(conf)

target_df = pd.read_csv(src_file)
column_names = target_df.columns.values.tolist()

for index, row in target_df.iterrows():
    message = { col: row[col] for col in column_names }
    producer.produce(topic,key=row['customer_state'],value=json.dumps(message),callback=acked)
    producer.poll(1)
