# Get Data from Spotify 

from identification import * 

import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
from pprint import pprint 
import json
from datetime import datetime
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))


pid = "spotify:playlist:37i9dQZEVXbNxXF4SkHj9F"
offset = 0 

response = sp.playlist_tracks(pid)
tracks = response['items']
today = datetime.today().strftime('%Y-%m-%d')
spotipy_data = []

print(len(tracks))
    
tracks.extend(response['items'])
uris = [track['track']['uri'] for track in tracks]
schema = ['date', 'position', 'song', 'artist', 'popularity', 'duration_ms', 'album_type', 'total_tracks', 'release_date', 'is_explicit', 'album_cover_url']
    
for rank, uri in enumerate(uris):
    track = sp.track(uri)
    song = track['name']
    artist_list = [artist['name'] for artist in track['album']['artists']]
        # spotify 에서 artist 정보가 여러개일 수 있음 
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

# album_names = []
# artist_names = []
# response = sp.playlist_items(playlist_id=pid,
#                                  offset=offset,
#                                  additional_types=['track'])



# for item in response['items']:
#     artist_list = []
        
#     for artist in item['track']['album']['artists']:
#         artist_list.append(artist['name'])
#     artist_names.append(artist_list)
#     album_names.append(item['track']['album']['name'])



# result= dict()
# for i in range(50):
#     temp = dict()
#     temp['album'] = album_names[i]
#     temp['artist'] = artist_names[i][0]

#     result[i] = temp

# print(result)
