import configparser
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import pymysql

# read config file
config = configparser.ConfigParser()
config.read('config.ini')

#set config
class Config:
    MYSQL_HOST = config['mysql']['host']
    MYSQL_PORT = config['mysql'].getint('port')
    MYSQL_USER = config['mysql']['user']
    MYSQL_PASSWORD = ""
    MYSQL_DB = config['mysql']['db']
    MYSQL_CHARSET = 'utf8mb4'

DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USER_ID = config['spotify']['user_id']
TOKEN = config['spotify']['token']

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

# Load dataframe to mysql
def dataframe_to_mysql(df: pd.DataFrame):
    try:
        #connect to sql
        connection = pymysql.connect(host=Config.MYSQL_HOST,
                                    port=Config.MYSQL_PORT,
                                    user=Config.MYSQL_USER,
                                    password=Config.MYSQL_PASSWORD,
                                    db=Config.MYSQL_DB,
                                    charset=Config.MYSQL_CHARSET)

        cursor = connection.cursor()

        # insert data
        for(row,s) in df.iterrows():
            song = s[0]
            artist = s[1]
            played_at = s[2]
            timestamp = s[3]
            
            query = f'INSERT INTO my_song VALUES("{song}","{artist}","{played_at}","{timestamp}")'
            cursor.execute(query)

        connection.commit()
        cursor.close()
    except:
        print('Error in Mysql connect')
    finally:
        connection.close()

if __name__ == '__main__':

    # EXTRACT
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {token}'.format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
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


    # LOAD
    dataframe_to_mysql(song_df)
    print("Data transferred to mysql successfully")