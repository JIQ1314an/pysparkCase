# Kafka主题名称
import time

import numpy as np
import pandas as pd

# pip install pykafka
from pykafka import KafkaClient

if __name__ == "__main__":
    # Kafka主题名称
    KAFKA_TOPIC_NAME_CONS = "songTopic"
    # Kafka地址
    KAFKA_BOOTSTRAP_SERVERS_CONS = "node120:9092"

    print("Kafka producer staring ... ")
    client = KafkaClient(hosts=KAFKA_BOOTSTRAP_SERVERS_CONS)
    topic = client.topics[KAFKA_TOPIC_NAME_CONS]
    # 包含 Spotify 数据的 csv。
    # 字段说明：
    # id 主键
    # name 歌曲名称
    # popularity 人气
    # duration_ms 歌曲时长
    # explicit 是否包含露骨内容
    # artists 艺术家
    # id_artists 创建该曲目的艺术家的 ID
    # release_date 发布日期
    # danceability 歌曲可跳舞程度的程度[0,1]
    # energy 歌的活力[0,1]
    # key 曲目的主要音符
    # loudness 音量/响度【db中歌曲的音量有多大】
    # mode track的模态，0表示次要，1表示主要
    # speechiness 音轨中存在口语单词的比例[0,1]
    # acousticness 声学程度[0,1]
    # instrumentalness 音轨中没有人声的比例[0,1]
    # liveness 观众在场[0,1]
    # valence 轨道的正性[0,1]
    # tempo BPM 中音轨的整体节奏
    # time_signature 拍号（几乎每首曲目都有 4 个）
    filepath = "D:\\大三下\\Big_Data_Application_Spark\\datasets\\tracks.csv"
    # 使用pandas读取文件数据文件
    songs_df = pd.read_csv(filepath)
    # 数据预处理
    # 过滤出人气大于50的数据
    songs_df = songs_df[songs_df["popularity"] > 50]
    songs_df["order_id"] = np.arange(len(songs_df))

    songs_df['name'] = songs_df['name'].str.replace(',', '', regex=True)
    songs_df['artists'] = songs_df['artists'].str.replace('[^a-zA-Z]', '', regex=True)
    songs_df['id_artists'] = songs_df['id_artists'].str.replace('[^a-zA-Z]', '', regex=True)

    # 形成JSON数据列表
    songs_list = songs_df.to_dict(orient="records")

    with topic.get_producer(delivery_reports=True) as producer:
        for message in songs_list:
            # message type is dict.
            message_fields_value_list = []
            message_fields_value_list.append(str(message["order_id"]))

            for field in list(message.keys())[:-1]:
                message_fields_value_list.append(str(message[field]))

            # str_message = ",".join(str(v) for v in message_fields_value_list)
            str_message = ",".join(message_fields_value_list)

            print("Message context: ", str_message)
            producer.produce(str_message.encode("utf-8"),
                             partition_key='{}'.format(message["order_id"] + 1).encode("utf-8"))
            time.sleep(1)
    print("Kafka producer program completed ... ")
