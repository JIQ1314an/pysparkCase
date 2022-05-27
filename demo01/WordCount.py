# coding:utf8
# 1. 导入Spark相关的包
from pyspark import SparkContext, SparkConf
# 问题解决：RuntimeError: Java gateway process exited before sending its port number
import os
os.environ['JAVA_HOME'] = "/opt/apps/jdk1.8.0_251"
# import os
# os.environ['PYSPARK_PYTHON'] = "D:\\Documents\\anaconda3\\envs\\ml\\python.exe"
if __name__ == '__main__':
    print('PySpark First Program')
    # TODO: 1. 当应用运行在集群上的时候，main函数就是Driver Program，必须创建SparkContext对象
    # 创建SparkConf对象，设置应用的配置信息，比如应用名称和应用运行模式
    conf = SparkConf().setAppName("WordCount").setMaster("local[*]")
    # TODO: 2. 构建SparkContext上下文实例对象，读取数据和调度Job执行
    sc = SparkContext(conf=conf)
    # 设置日志级别
    # sc.setLogLevel("INFO")
    # 第一步、读取本地数据 封装到RDD集合，认为列表List
    file_rdd = sc.textFile("./data/words.txt")
    # 第二步、处理数据 调用RDD中函数，认为调用列表中的函数
    # a. 每行数据分割为单词
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    # b. 转换为二元组，表示每个单词出现一次
    word_one_rdd = words_rdd.map(lambda x: (x, 1))
    # c. 按照Key分组聚合
    result_rdd = word_one_rdd.reduceByKey(lambda a, b: a + b)
    # 第三步、输出数据
    res_rdd_col2 = result_rdd.collect()
    # 输出到控制台
    for line in res_rdd_col2:
        print(line)

    print('停止 PySpark SparkSession 对象')
    # 关闭SparkContext
    sc.stop()