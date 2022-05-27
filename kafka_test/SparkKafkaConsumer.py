from pyspark.sql import SparkSession

from pyspark.sql.functions import col, from_csv


# pip install pykafka
class Comsumer:
    def __init__(self, spark):
        self.spark = spark

    def _console(self):  # 输出到控制台
        song_find_text = self.spark.sql("select count(1) from song_find")

        songs_agg_write_stream = song_find_text \
            .writeStream \
            .trigger(processingTime="5 seconds") \
            .outputMode("Complete") \
            .option("truncate", "false") \
            .format("console") \
            .start()  # truncate 截断

        songs_agg_write_stream.awaitTermination()

    def _table_output(self):  # 输出到新的表【testedTable1】中
        # 获取音乐数据
        song_find_text = self.spark.sql("SELECT * FROM song_find")
        # 输出到新的表中
        songs_agg_write_stream = song_find_text \
            .writeStream \
            .trigger(processingTime='5 seconds') \
            .outputMode("append") \
            .option("truncate", "false") \
            .format("memory") \
            .queryName("testedTable1") \
            .start()
        songs_agg_write_stream.awaitTermination(1)

    def start_consumer(self):
        # Kafka主题名称
        kafka_topic_name = "songTopic"
        # Kafka地址
        kafka_bootstrap_servers = "node120:9092"

        songs_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic_name) \
            .option("startingOffsets", "latest") \
            .load()

        songs_df1 = songs_df.selectExpr("CAST(value AS STRING)", "timestamp")
        songs_schema_string = "order_id INT, id STRING, name STRING, popularity INT, duration_ms DOUBLE, explicit INT, " \
                              + "artists STRING, id_artists STRING, release_date STRING, " \
                              + "danceability DOUBLE," \
                              + "energy DOUBLE, key INT, loudness DOUBLE, " \
                              + "mode INT," \
                              + "speechiness DOUBLE," \
                              + "acousticness DOUBLE, instrumentalness DOUBLE, liveness DOUBLE, " \
                              + "valence DOUBLE, tempo DOUBLE, time_signature DOUBLE"

        songs_df2 = songs_df1 \
            .select(from_csv(col("value"), songs_schema_string) \
                    .alias("song"), "timestamp")

        songs_df3 = songs_df2.select("song.*", "timestamp")
        songs_df3.createOrReplaceTempView("song_find")

        print("Spark Consuming...")
        self._table_output()
        # self._console()

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("streaming") \
        .master("local[4]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Comsumer(spark).start_consumer()
