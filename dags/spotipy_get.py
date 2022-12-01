import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import psycopg2

from sqlalchemy import create_engine

SPOTIPY_CLIENT_ID = '53d09728d8a44ac3b650d1ee8fcf5e2b'
SPOTIPY_CLIENT_SECRET ='800720ee113a489e9640e37c60b5f94a'
SPOTIPY_REDIRECT_URI = 'https://example.com/callback/'

auth_manager = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
spot = spotipy.Spotify(auth_manager=auth_manager)

def call_playlist(creator, id_playlist, country):
    
    #step1

    list_features_playlist = ["artist","album","track_name","popularity",  "track_id","danceability","energy","key","loudness","mode", "speechiness","instrumentalness","liveness","valence","tempo", "duration_ms","time_signature"]
    
    playlist_df = pd.DataFrame(columns = list_features_playlist)
    
    #step2
    
    playlist = spot.user_playlist_tracks(creator, id_playlist, market=country)["items"]
    for track in playlist:
        # Create empty dict
        playlist_feat = {}
        # Get metadata
        playlist_feat["artist"] = track["track"]["album"]["artists"][0]["name"]
        playlist_feat["album"] = track["track"]["album"]["name"]
        playlist_feat["track_name"] = track["track"]["name"]
        playlist_feat["popularity"] = track["track"]["popularity"]
        playlist_feat["track_id"] = track["track"]["id"]
        
        # Get audio features
        audio_features = spot.audio_features(playlist_feat["track_id"])[0]
        for feature in list_features_playlist[5:]:
            playlist_feat[feature] = audio_features[feature]
        
        # Concat the dfs
        track_df = pd.DataFrame(playlist_feat, index = [0])
        playlist_df = pd.concat([playlist_df, track_df], ignore_index = True)

    #Step 3
    #playlist_df.to_csv('/opt/airflow/data/playlistKpopps.csv') 
    conn_string = 'postgresql://postgres:maholtra26@host.docker.internal/dbrekdat22'
  
    db = create_engine(conn_string)
    conn = db.connect()
    #playlist_df.to_csv("/opt/airflow/data/playlistKpop.csv")

    playlist_df.to_sql('spotipyy', con=conn, if_exists='append',
          index=False)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    conn.close()
    return playlist_df  
    
#df = call_playlist("spotify","37i9dQZEVXbObFQZ3JLcXt","ID")

# print(df)
# playlist = sp.user_playlist_tracks('spotify', '37i9dQZEVXbMDoHDwVN2tF', limit=5, market='ID')["items"]
# print(playlist)

# df_csv = df.to_csv('playlist.csv', index = True)