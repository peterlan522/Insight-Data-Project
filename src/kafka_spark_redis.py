# Kafka: 1.0.0
# Spark: 2.2.1
# Zookeeper: 3.4.10
# Scala: 2.11
# Python: 2.7
# Redis: 3.2.6

import json
from collections import Counter, deque
from os import getenv
import redis
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from afinn import Afinn

import sys
reload(sys)
sys.setdefaultencoding('utf8')

APP_NAME = "What's-Hot"
SPARK_MASTER = "spark://ip-10-0-0-11:7077"
KAFKA_TOPIC = ["topic-twitter"]
KAFKA_BROKERS = "10.0.0.9:9092,10.0.0.5:9092,10.0.0.6:9092"
REDIS_HOST = "35.170.1.59"
REDIS_PORT = 6379
BATCH_LAYER = "s3n://peter-data-output/file"
CHECKPOINT_DIR = "s3n://peter-data-checkpoint/spark"
BATCH_SIZE = 1
WINDOW_SIZE = 600 * BATCH_SIZE
FREQUENCY = 60 * BATCH_SIZE
REDIS_SORTED_SET = "tweets"


def storeToRedis(partition):
    conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    conn.flushdb()
    pipe = conn.pipeline()
    for record in partition:
        pipe.zadd(REDIS_SORTED_SET, -
                  record[1][0], [record[0], record[1][1], record[1][2], record[1][3]])
    pipe.execute()


def scoreToList(score):
    if score > 0:
        return (1, 1, 0, 0)
    elif score < 0:
        return (1, 0, 1, 0)
    else:
        return (1, 0, 0, 1)


# spark streaming pull message from kafka brokers
def main():

    conf = SparkConf().setAppName(APP_NAME).setMaster(SPARK_MASTER)
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, BATCH_SIZE)
    ssc.checkpoint(CHECKPOINT_DIR)

    afinn = Afinn()

    topic = KafkaUtils.createDirectStream(
        ssc, KAFKA_TOPIC, {"metadata.broker.list": KAFKA_BROKERS})

    parsed = topic.map(lambda v: json.loads(v[1])) \
        .filter(lambda tweet: 'text' in tweet and len(tweet['text']) > 0) \
        .filter(lambda tweet: 'timestamp_ms' in tweet and len(tweet['timestamp_ms']) > 0) \
        .filter(lambda tweet: 'entities' in tweet and len(tweet['entities']['hashtags']) > 0 ) \
        .map(lambda t: (t['entities']['hashtags'][0]['text'].lower(), afinn.score(t['text'])))

    addFun = lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])
    invFun = lambda a, b: (a[0] - b[0], a[1] - b[1], a[2] - b[2], a[3] - b[3])

    tags = parsed.map(lambda t: (t[0], scoreToList(t[1]))) \
                 .reduceByKeyAndWindow(addFun, invFun, WINDOW_SIZE, FREQUENCY) \
                 .transform(lambda rdd: rdd.sortBy(lambda a: -a[1][0]))

    saveTag = parsed.saveAsTextFiles(BATCH_LAYER)

    tags.pprint()

    tags.foreachRDD(lambda rdd: rdd.foreachPartition(
        lambda p: storeToRedis(p)))

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
