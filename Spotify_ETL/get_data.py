# Get Data from Spotify 

from identification import * 

import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
from pprint import pprint 



sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))


pid = "spotify:playlist:37i9dQZEVXbNxXF4SkHj9F"
offset = 0 

album_names = []
while True:
    response = sp.playlist_items(playlist_id=pid,
                                 offset=offset,
                                 additional_types=['track'])
    
    for item in response['items']:
        artist_names = []
        
        for artist in item['track']['album']['artists']:
            artist_names.append(artist['name'])
        
        album_names.append(item['track']['album']['name'])

    
    break 

# 50 개 다 수집되었는지 확인 
print(len(album_names))