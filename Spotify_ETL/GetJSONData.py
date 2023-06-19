# JSON 파일 형식으로 데이터 받아오기

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from identification import *
from datetime import datetime
import json

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))

kr_playlist_id = "spotify:playlist:37i9dQZEVXbNxXF4SkHj9F" # Korea Spotify Top 50
jp_playlist_id = "spotify:playlist:37i9dQZEVXbKXQ4mDTEBXq" # Japan Spotify Top 50

today = datetime.today().strftime('%Y-%m-%d')
data = {}
data[today] = []
response = sp.playlist_tracks(jp_playlist_id)
tracks = response['items']
data[today].append(tracks)


# .json 파일로 export 하기
file_path = "./jp_playlist2.json"
print(data)

with open(file_path, "w") as outfile:
    json.dump(data, outfile)