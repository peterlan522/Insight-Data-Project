from multiprocessing import Pool
import time
import json
from kafka import SimpleProducer, KafkaClient
import re

INPUT_TOPIC = "topic-twitter"
FILE_PATH = "/home/ubuntu/tweets.json"
KAFKA_SERVER = "10.0.0.9:9092,10.0.0.5:9092,10.0.0.6:9092"


def worker(topic, file):
    tweets = []
    with open(file, 'r') as f:
        tweets = json.load(f)

    kafka = KafkaClient(KAFKA_SERVER)
    producer = SimpleProducer(kafka)
    while True:
        for t in tweets:
            producer.send_messages(topic, t.encode('utf-8'))


def main():
    p = Pool(5)
    p.apply(worker, args=(INPUT_TOPIC, FILE_PATH))
    p.start()


if __name__ == '__main__':
    main()
