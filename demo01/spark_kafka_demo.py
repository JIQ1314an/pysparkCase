from pyspark.streaming.kinesis import KinesisUtils
from pykafka import KafkaClient

if __name__ == "__main__":
    client = KafkaClient(hosts="node120:9092")
    topic = client.topics["t1"]
    with topic.get_sync_producer() as producer:
        # for i in range(4):
        #     producer.produce(bytes("test message" + str(i ** 2), encoding='utf-8'))
        while (1):
            producer.produce(bytes(input("Enter a line of data and end with return:\n"), encoding='utf-8'))

    # consumer = topic.get_simple_consumer()
    # for message in consumer:
    #     if message is not None:
    #         print(message.offset, message.value)
