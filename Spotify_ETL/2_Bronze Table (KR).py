# Databricks notebook source
pip install spotipy

# COMMAND ----------

# Get Data from Spotify 

from identification import * 

import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials

from datetime import date, timedelta, timezone, datetime

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))


pid = "spotify:playlist:37i9dQZEVXbNxXF4SkHj9F"
offset = 0 

response = sp.playlist_tracks(pid)
tracks = response['items']
today = datetime.today().strftime('%Y-%m-%d')
spotipy_data = []

print(len(tracks))

uris = [track['track']['uri'] for track in tracks]
schema = ['date', 'position', 'song', 'artist', 'popularity', 'duration_ms', 'album_type', 'total_tracks', 'release_date', 'is_explicit', 'album_cover_url']
    
for rank, uri in enumerate(uris):
    track = sp.track(uri)
    song = track['name']
    artist_list = [artist['name'] for artist in track['album']['artists']]

    if len(artist_list) > 1:
        artist = artist_list[0] + ' & ' + ' & '.join(artist_list[1:])
    else:
        artist = artist_list[0]
    popularity = track['popularity']
    duration_ms = track['duration_ms']
    album_type = track['album']['album_type']
    total_tracks = track['album']['total_tracks']
    release_date = track['album']['release_date']
    is_explicit = track['explicit']
    album_cover_url = track['album']['images'][0]['url']

    spotipy_data.append([today, rank+1, song, artist, popularity, duration_ms, album_type, total_tracks, release_date, is_explicit, album_cover_url])

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(spotipy_data, columns= schema)

# COMMAND ----------

sparkDF = spark.createDataFrame(df)
sparkDF.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Bronze Table  
# MAGIC - Create Bronze Table by transforming spark dataframe to sql table 
# MAGIC - Save raw data as 'spotify_bronze' table 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE spotify_bronze_KR
# MAGIC (date STRING, position LONG, song STRING, artist STRING, popularity LONG, duration_ms LONG, album_type STRING, total_tracks LONG, release_date STRING, is_explicit BOOLEAN, album_cover_url STRING);

# COMMAND ----------

sparkDF.write.mode('overwrite').saveAsTable('spotify_bronze_KR')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM spotify_bronze_KR;
