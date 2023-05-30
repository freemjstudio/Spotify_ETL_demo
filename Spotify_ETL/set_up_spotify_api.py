# Databricks notebook source
pip install spotipy

# COMMAND ----------

# Get Data from Spotify 

from identification import * 

import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
from pprint import pprint 
import pandas as pd 


sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))


pid = "spotify:playlist:37i9dQZEVXbNxXF4SkHj9F"
offset = 0 

album_names = []
while True:
    response = sp.playlist_items(playlist_id=pid,
                                 offset=offset,
                                 additional_types=['track'])


    data = pd.DataFrame.from_dict(response)
    break 


# COMMAND ----------

data

# COMMAND ----------

# data 파싱하기 
items = data['items'] # 각각의 object는 json 형식 

playlist_df = dict()
for item in items:
    item_df = pd.DataFrame.from_dict(item)
    item_df['album']
    #item_df['artist']

# COMMAND ----------



# COMMAND ----------

   # pd_object = pd.read_json(response, type='series')
    # for item in response['items']:
    #     artist_names = []
        
    #     for artist in item['track']['album']['artists']:
    #         artist_names.append(artist['name'])
        
    #     album_names.append(item['track']['album']['name'])

