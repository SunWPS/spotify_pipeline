def getDataFromSpotify():
    import pandas as pd
    import requests
    import json
    from datetime import datetime, timedelta


    def check_valid_data(df: pd.DataFrame):
            # check if df is empty
        if df.empty:
            print("No songs dowloaded.")
            return False

        # check if primary key isn't unique
        if pd.Series(df['played_at']).is_unique:
            pass
        else:
            raise Exception("PK check is violated")  

        if df.isnull().values.any():
            raise Exception("Null valued found")

        return True

    DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
    USER_ID = "" #user_id
    TOKEN = "" #token
    
    # EXTRACT
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {token}'.format(token=TOKEN)
    }

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    url = 'https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp)

    r = requests.get(url, headers=headers)

    data = r.json()

    songs = []
    artists = []
    played_at = []
    timestamps = []

    for song in data['items']:
        songs.append(song['track']['name'])
        artists.append(song['track']['album']['artists'][0]['name'])
        played_at.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])
           
    song_dict = {
        'song': songs,
        'artist': artists,
        'played_at': played_at,
        'timestamp': timestamps
    }

    song_df = pd.DataFrame(song_dict, 
                            columns = ['song', 'artist', 'played_at', 'timestamp'])


    # TRANSFORM
    if check_valid_data(song_df):
        print("Data valid")
        
     # get only song that played yesterday
    excep = pd.to_datetime(song_df['timestamp'], format='%Y-%m-%d') == str(yesterday)[0:10]
    song_df = song_df[excep]
    path='~/store_files_airflow/mySong.csv'
    song_df.to_csv(path, index=False)


