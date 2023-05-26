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
                                 additional_types=['track'])
    
    
    break 
    # if len(response['track']['items']) == 0:
    #     break 

    # print(response['track']['items'])
    # offset = offset + len(response['track']['items'])
    # print(offset, "/", response["total"])