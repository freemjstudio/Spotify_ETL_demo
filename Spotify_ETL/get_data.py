# Get Data from Spotify 

from identification import * 

import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
from pprint import pprint 



sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET))


pid = "spotify:playlist:37i9dQZEVXbNxXF4SkHj9F"
offset = 0 

playlist_list = []

while True:
    response = sp.playlist_items(playlist_id=pid,
                                 offset=offset,
                                 additional_types=['track'], limit=1)
    
    for item in response['items']:
        print(item['track']['album'])
        print(item['track']['album']['name']) # 가수 이름 

    
    break 