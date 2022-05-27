import time

import pandas as pd
from pyspark.sql import SparkSession
from kafka_test.SparkKafkaConsumer import Comsumer
from spotifyRec.recommender import SpotifyRecommender
from spotifyRec.song_data_processing import SongDataProcessor


def start_spark():
    spark = SparkSession \
        .builder \
        .appName("streaming") \
        .master("local[4]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_filter_data(df):
    df = df.sort(df.release_date.desc())
    # 从采集的数据集中删除不需要的列
    df = df.drop('order_id', 'id', 'explicit', 'release_date',
                 'id_artists', 'time_signature', 'duration_ms', 'timestamp')

    add_df = SongDataProcessor().get_random_song()
    # 将我们喜欢的音乐加入到这个数据集中  由于两者列名顺序不同，需要使用unionByName(类型按照为df原先的类型)
    union_df = df.unionByName(spark.createDataFrame(add_df))
    assembled_data = SongDataProcessor().feature_vectorise(union_df)
    output_df = SongDataProcessor().kmeans_cluster(assembled_data)
    data_df = SongDataProcessor().rec_prior_preparation(output_df)

    liked_song_name = add_df.iloc[-1]['name']  # 获取用户收藏音乐的名称
    value_cate = data_df.iloc[-1]['prediction']  # 用户收藏音乐对应的类别（k-means预测的）
    # print(liked_song_name, value_cate)

    filtered_data = data_df[data_df['prediction'] == 1]
    return liked_song_name, filtered_data


if __name__ == "__main__":
    # 推荐歌曲数量
    recommend_num = 10
    # 启动spark
    spark = start_spark()
    # 启动消费者
    Comsumer(spark).start_consumer()
    # 测试是否将采集的数据存储到表中
    df = spark.sql("SELECT * FROM testedTable1")
    # 当从kafka拿到的数据记录大于等于10的recommend_num时，启动推荐程序，否者一直不停去查询
    min_batch = recommend_num * 10
    while df.count() < min_batch:
        # print("Start : %s" % time.ctime())
        time.sleep(2)  # 2s
        df = spark.sql("SELECT * FROM testedTable1")
    print("*****************Running Recommend Program*****************")
    # 执行推荐程序
    liked_song_name, filtered_data = get_filter_data(df)
    recommender = SpotifyRecommender(filtered_data)
    rec_song = recommender.spotify_recommendations(recommend_num)

    print("liked song: {}\nrecommend songs: {}"
          .format(liked_song_name, ",".join(rec_song["name"].values)))
