from confluent_kafka import Consumer,KafkaError,KafkaException
import sys
import json
import pandas as pd
import time

MIN_COMMIT_COUNT = 20

def basic_consume_loop(consumer, topics,msg_processor):
    rows =  list()
    try:
        msg_count = 0
        consumer.subscribe(topics)
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None: 
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                rows.append(msg_processor(msg))
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        return rows


def parse_msg(message):
    message_data = json.loads(message.value().decode('utf-8'))
    return message_data



def get_data_from_kafka():
    conf = {'bootstrap.servers': '192.168.56.131:9092',
            'group.id': f'assignment_{round(time.time())}',
            'auto.offset.reset': 'smallest'}
    consumer = Consumer(conf)
    data = basic_consume_loop(consumer,["assignment",],parse_msg)
    return pd.DataFrame(data)


