import sys

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth


def get_song_data():
    # 设置spotipy应用接口参数   me
    CLIENT_ID = "85abc35b96044909a7f9271e944604c7"
    CLIENT_SECRET = "87da784091c94768997ba35ae54c995f"
    REDIRECT_URI = "https://cn.bing.com/"
    SCOPE = "user-library-read"

    # 获取SpotifyApi对象
    sp_api = SpotifyApi(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, SCOPE)
    # 根据操作对象获取当前用户所收藏的曲目列表【最多查询50首】
    song_data = sp_api.get_track_df(max_query_num=50)

    return song_data


class SpotifyApi:
    def __init__(self, client_id, client_secret, redirect_uri, scope):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scope = scope

    def connect_to_spotify(self):
        token = SpotifyOAuth(client_id=self.client_id,
                             client_secret=self.client_secret,
                             redirect_uri=self.redirect_uri,
                             scope=self.scope)
        # print(token)
        if not token:
            sys.exit('Authorization failed')
        sp = spotipy.Spotify(auth_manager=token)
        return sp

    # 只保存有用的字段数据
    def _save_only_some_fields(self, track_response):
        return {
            'id': str(track_response['track']['id']),
            'name': str(track_response['track']['name']),
            'artists': [artist['name'] for artist in track_response['track']['artists']],
            'duration_ms': track_response['track']['duration_ms'],
            'popularity': track_response['track']['popularity'],
            'added_at': track_response['added_at']
        }

    def _add_other_features(self, spotipy_obj, tracks_df):
        # generate features  dict
        audio_features = {}
        for idd in tracks_df['id']:
            audio_features[idd] = spotipy_obj.audio_features(idd)[0]

        # 根据需求更改 unneeded_features
        unneeded_features = {"type", "id", "uri", "track_href", "analysis_url", "duration_ms"}
        all_features = set(audio_features[set(audio_features.keys()).pop()].keys())
        reserved_features = list(all_features.difference(unneeded_features))
        # print(reserved_features)

        for feature in reserved_features:
            tracks_df[feature] = tracks_df['id'].apply(lambda idd: audio_features[idd][feature])

        return tracks_df

    def get_track_df(self, max_query_num):
        """
        :param max_query_num: Get the max number of the current user's favorite track list.
        :return: DataFrame : User's favorite songs and corresponding features
        """
        # 连接到spotify并获得操作对象
        spotipy_obj = self.connect_to_spotify()
        # 根据操作对象获取当前用户所收藏的曲目列表【limit：最多查询 max_query_num 首】
        saved_tracks_resp = spotipy_obj.current_user_saved_tracks(limit=max_query_num)

        # 输出用户当前收藏的音乐数（小于等于max_query_num）
        # number_of_tracks = saved_tracks_resp["total"]
        # print("共获取 %d 首音乐" % number_of_tracks)

        tracks = [self._save_only_some_fields(track) for track in saved_tracks_resp['items']]

        # 可选项
        while saved_tracks_resp['next']:
            saved_tracks_resp = spotipy_obj.next(saved_tracks_resp)
            tracks.extend([self._save_only_some_fields(track) for track in saved_tracks_resp['items']])

        # 转换为DataFrame
        tracks_df = pd.DataFrame(tracks)
        # 转换列的数据
        tracks_df['artists'] = tracks_df['artists'].apply(lambda artists: artists[0])
        tracks_df['duration_ms'] = tracks_df['duration_ms'].apply(lambda duration: duration / 1000)
        tracks_df = tracks_df.rename(columns={'duration_ms': 'duration_s'})

        return self._add_other_features(spotipy_obj, tracks_df)


if __name__ == "__main__":
    # 设置spotipy应用接口参数   me
    CLIENT_ID = "85abc35b96044909a7f9271e944604c7"
    CLIENT_SECRET = "87da784091c94768997ba35ae54c995f"
    REDIRECT_URI = "https://cn.bing.com/"
    SCOPE = "user-library-read"

    # 获取SpotifyApi对象
    sp_api = SpotifyApi(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, SCOPE)
    # 根据操作对象获取当前用户所收藏的曲目列表【最多查询50首】
    song_data = sp_api.get_track_df(max_query_num=50)
    # print(song_data)

    number_of_tracks = len(song_data)
    print("共获取 %d 首音乐" % number_of_tracks)

    # # 连接到spotify并获得操作对象
    # spotipy_obj = sp_api.connect_to_spotify()
    # # 根据操作对象获取当前用户所收藏的曲目列表【limit：最多查询50首】
    # saved_tracks_resp = spotipy_obj.current_user_saved_tracks(limit=50)
    #
    # number_of_tracks = saved_tracks_resp["total"]
    # print("共获取 %d 首音乐" % number_of_tracks)
    #
    # for idx, item in enumerate(saved_tracks_resp['items']):
    #     track = item['track']
    #     print(idx, track['artists'][0]['name'], " – ", track['name'])
