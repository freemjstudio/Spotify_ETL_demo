# Get Data from Spotify 

from identification import * 

import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
from pprint import pprint 
import json

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))


pid = "spotify:playlist:37i9dQZEVXbNxXF4SkHj9F"
offset = 0 


album_names = []
artist_names = []
response = sp.playlist_items(playlist_id=pid,
                                 offset=offset,
                                 additional_types=['track'])

for item in response['items']:
    artist_list = []
        
    for artist in item['track']['album']['artists']:
        artist_list.append(artist['name'])
    artist_names.append(artist_list)
    album_names.append(item['track']['album']['name'])



result= dict()
for i in range(50):
    temp = dict()
    temp['album'] = album_names[i]
    temp['artist'] = artist_names[i][0]

    result[i] = temp

print(result)