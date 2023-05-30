# Databricks notebook source
pip install spotipy

# COMMAND ----------

# Get Data from Spotify 

from token import * 

import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from datetime import date, timedelta, timezone, datetime

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))

playlist_id_list = ["spotify:playlist:37i9dQZEVXbNxXF4SkHj9F", "spotify:playlist:37i9dQZEVXbKXQ4mDTEBXq", "spotify:playlist:37i9dQZEVXbLRQDuF5jeBp"] # KR - JP 
conutry_info = {0:"Korea", 1:"Japan", 2:"USA"}
country_code = {0:"KR", 1:"JP", 2:"USA"}

for idx in len(playlist_id_list): 
    pid = playlist_id_list[idx]
    response = sp.playlist_tracks(pid)
    tracks = response['items']
    today = datetime.today().strftime('%Y-%m-%d')
    spotipy_data = []

    if len(tracks) == 50:
        print("The list number of the playlists", len(tracks))
    else:
        print("ERROR", len(tracks))
        

    uris = [track['track']['uri'] for track in tracks]
    schema = ['date', 'position', 'song', 'artist', 'popularity', 'duration_ms', 'album_type', 'total_tracks', 'release_date', 'is_explicit', 'album_cover_url', 'playlist_country']
    
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
        playlist_country = conutry_info[idx]

        spotipy_data.append([today, rank+1, song, artist, popularity, duration_ms, album_type, total_tracks, release_date, is_explicit, album_cover_url, playlist_country]) 

        # CREATE DATAFRAME 
    
        df = pd.DataFrame(spotipy_data, columns= schema)    
        sparkDF = spark.createDataFrame(df)
        sparkDF.write.mode('overwrite').saveAsTable(f'spotify_bronze_{country_code[idx]}')


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Bronze Table  
# MAGIC - Create Bronze Table by transforming spark dataframe to sql table 
# MAGIC - Save raw data as 'spotify_bronze' table 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS spotify_bronze_KR
# MAGIC (date STRING, position LONG, song STRING, artist STRING, popularity LONG, duration_ms LONG, album_type STRING, total_tracks LONG, release_date STRING, is_explicit BOOLEAN, album_cover_url STRING, playlist_country STRING);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS spotify_bronze_JP
# MAGIC (date STRING, position LONG, song STRING, artist STRING, popularity LONG, duration_ms LONG, album_type STRING, total_tracks LONG, release_date STRING, is_explicit BOOLEAN, album_cover_url STRING, playlist_country STRING);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS spotify_bronze_USA
# MAGIC (date STRING, position LONG, song STRING, artist STRING, popularity LONG, duration_ms LONG, album_type STRING, total_tracks LONG, release_date STRING, is_explicit BOOLEAN, album_cover_url STRING, playlist_country STRING);

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Check Quality 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM spotify_bronze_KR;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM spotify_bronze_JP;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM spotify_bronze_USA;
