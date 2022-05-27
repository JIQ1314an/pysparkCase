from pyspark import SparkContext, SparkConf

# import os
# os.environ['JAVA_HOME'] = "/opt/apps/jdk1.8.0_251"

if __name__ == "__main__":
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf=conf)
    # 一行一行读取, RDD
    # result = sc.textFile("./data/words.txt") \
    result = sc.textFile("hdfs://node120:9000/spark/data/words.txt") \
                .flatMap(lambda line: line.split(" ")) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .collect()
    print(result)
    sc.stop()

