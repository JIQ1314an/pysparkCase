from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    sc = SparkContext("local[2]","haha")
    sc.setLogLevel("ERROR")
    # 批处理间隔为 5 秒
    ssc = StreamingContext(sc, 5)
    # 需求一：读取网络端口流数据
    # # 1. 使用这个上下文，我们可以创建一个表示来自 TCP 源的流数据的 DStream，指定为主机名（node120）和端口（例如8888）。
    ds = ssc.socketTextStream("node120", 8888)
    # 1. 创建文件数据
    # ds = ssc.textFileStream("hdfs://node120:9000/spark/stream/file/in")
    # 2. 统计词频
    result = ds \
        .flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda v1, v2: v1 + v2)

    # 3. 将打印每5秒生成的一些计数
    result.pprint()
    # 4. 启动采集器
    ssc.start()
    # 5. 让采集器一直运行不结束
    ssc.awaitTermination()
