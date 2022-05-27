from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from spotifyRec import spotify_api
import random
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


class SongDataProcessor:
    def __init__(self):
        pass

    def get_random_song(self):
        # get song data
        song_data = spotify_api.get_song_data()
        song_data = song_data.drop(['id', 'added_at', 'time_signature', 'duration_s'], axis='columns')
        # 从我们的 Spotify 喜欢的歌曲播放列表中检索一首随机歌曲
        rand_n = random.randint(0, len(song_data) - 1)
        # get DataFrame object
        add_df = song_data.iloc[rand_n: rand_n + 1, :]
        # print(add_df.columns)
        # add_df
        return add_df

    def feature_vectorise(self, union_df):
        indexer = StringIndexer(inputCol='artists', outputCol='artistsIndex', handleInvalid='keep')
        model = indexer.fit(union_df)
        idexed = model.transform(union_df)
        # idexed.show(1)

        # features "name" and " artists" not add
        input_cols = idexed.columns[1:]
        input_cols.remove("artists")
        # input_cols

        # VectorAssembler是将给定列列表组合成单个向量列的转换器。为了训练逻辑回归和决策树等ML模型，将原始特征和不同特征转换器生成的特征组合成一个特征向量是很有用的。VectorAssembler接受以下输入列类型:所有数值类型、布尔类型和向量类型。在每一行中，输入列的值将按照指定的顺序连接到一个向量中。
        assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
        # 模型训练
        assembled_data = assembler.setHandleInvalid("skip").transform(idexed)
        # assembled_data.head()

        return assembled_data

    def rec_prior_preparation(self, output_df):
        # 抛弃那个feature vector
        columns_to_drop = ["features", "standardized"]
        # spark sql DataFrame
        data_sdf = output_df.drop(*columns_to_drop)

        # print(len(data_sdf.columns))
        # python DataFrame
        data_df = data_sdf.toPandas()
        data_df.drop_duplicates(inplace=True)

        return data_df

    def kmeans_cluster(self, assembled_data):

        scale = StandardScaler(inputCol='features', outputCol='standardized')
        data_scale = scale.fit(assembled_data)
        df = data_scale.transform(assembled_data)

        evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', metricName='silhouette',
                                        distanceMeasure='squaredEuclidean')

        KMeans_algo = KMeans(featuresCol='standardized', k=3)
        KMeans_fit = KMeans_algo.fit(df)
        output_df = KMeans_fit.transform(df)
        # output_df
        return  output_df