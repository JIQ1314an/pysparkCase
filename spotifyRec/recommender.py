import numpy as np
import pandas as pd


class SpotifyRecommender:

    def __init__(self, rec_data):
        self.rec_data = rec_data

    def spotify_recommendations(self, amount=1):
        if len(self.rec_data) - 1 <= amount and len(self.rec_data) > 1:
            # 直接全部推荐（除最后一行数据，它是用户收藏的歌曲）
            # len(self.rec_data)==1 表示用户收藏的歌曲自成一类（k-means）,此时需直接按距离计算
            # 同时移除一些生成字段，即最后两个字段artistsIndex、prediction
            return self.rec_data.iloc[:-1, :-2]

        # 否则计算距离
        # 非数字列不需要考虑， 由于 prediction 相同也不考虑。
        num_features = self.rec_data.drop(columns=['name', 'artists', 'prediction'])
        # 获取我们最喜欢的歌曲的详细信息（数值型，因为我们之前缀加到最后一行，所以直接取最后一行）Series类型
        liked_song = num_features.iloc[-1, :]
        # 删除我们最喜欢的歌曲的数据，这样它就不会影响我们的推荐（直接去掉最后一行即可，因为之前做过去重操作）。
        may_recommend_songs = num_features.iloc[:-1, :]
        # 计算曼哈顿距离
        distance_vector = np.sum(np.abs(may_recommend_songs - liked_song), axis=1).rename("distance")
        # print(distance_vector)

        # 距离被计算并附加并添加到我们数据集中称为距离的新列中.
        # 由于distance_vector比self.rec_data少最后一列，所以合并后的结果，最后一列的distance特征为Nan
        temp_result = pd.concat([self.rec_data, distance_vector], axis=1)
        # print(temp_result)

        # 将我们的数据排序为按“距离”特征升序
        result_data = temp_result.sort_values('distance')

        # 结果数据集的歌曲与我们最喜欢的歌曲的数值相似，因此被推荐
        # 同时移除一些生成字段，即最后三个字段artistsIndex、prediction、distance
        return result_data.iloc[:amount, :-3]
