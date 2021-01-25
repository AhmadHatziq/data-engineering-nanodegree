import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *

def extract_song(df):
    """
    Extracts the song info from the dataframe. 
    
    Args:
        param1 (df): Dataframe with the following columns: artist_id, artist_latitude, artist_location, artist_longitude, 
                                                            artist_name, duration, num_songs, song_id, title, year
    Returns: 
        list of the form [song_id, title, artist_id, year, duration]
    """
    
    # Extract respective values
    artist_id = df[['artist_id']].values[0][0]
    song_id = df[['song_id']].values[0][0]
    title = df[['title']].values[0][0]
    year = df[['year']].values[0][0].item() 
    duration = df[['duration']].values[0][0]
    
    # Create and returns the list
    song_data = [song_id, title, artist_id, year, duration]
    return song_data

def extract_artist(df):
    """
    Extracts the artist info from the dataframe. 
    
    Args:
        param1 (df): Dataframe with the following columns: artist_id, artist_latitude, artist_location, artist_longitude, 
                                                            artist_name, duration, num_songs, song_id, title, year
    Returns: 
        list of the form [artist_id, name, location, latitude, longitude]
    """
    
    # Extract respective values
    artist_id = df[['artist_id']].values[0][0]
    name = df[['artist_name']].values[0][0]
    location = df[['artist_location']].values[0][0]
    latitude = df[['artist_latitude']].values[0][0]
    longitude = df[['artist_longitude']].values[0][0]

    # Create and returns the list
    artist_data = [artist_id, name, location, latitude, longitude]
    return artist_data

def process_song_file(cur, filepath):
    """
    Processes the .json song file. 
    
    Args:
        param1 (cur): Cursor
        param2 (filepath): Filepath to the json files eg 'data/song_data'
        
    Returns: Count of rows processed
    """
    # Ignores filepath containing the string 'checkpoint'
    if "checkpoint" in filepath: return 0
    
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = extract_song(df)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = extract_artist(df)
    cur.execute(artist_table_insert, artist_data)
    
    return len(df)


def process_log_file(cur, filepath):
    """
    Processes the .json log file. 
    
    Args:
        param1 (cur): Cursor
        param2 (filepath): Filepath to the json files eg 'data/log_data'
        
    Returns: Count of rows processed
    """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong'] 

    # convert timestamp column to datetime
    df['ts'] = df['ts'].astype(int)
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
    t = df[['datetime', 'ts']].copy()
    t['year'] = t['datetime'].dt.year
    t['month'] = t['datetime'].dt.month
    t['day'] = t['datetime'].dt.day
    t['hour'] = t['datetime'].dt.hour
    t['weekday_name'] = t['datetime'].dt.weekday_name
    t['week'] = t['datetime'].dt.week
    
    # insert time data records
    time_data = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday_name']
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = t[time_data]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent) 
        cur.execute(songplay_table_insert, songplay_data)
        
    return len(df)


def process_data(cur, conn, filepath, func):
    """
    Args:
        param1 (cur): Cursor
        param2 (conn): Connection
        param3 (filepath): Filepath to the json files eg 'data/song_data'
        param4 (func): Either process_song_file(...) or process_log_file(...)
        
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # Count the total number of rows processed
    total_rows = 0

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        total_rows = total_rows + func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        
    
    # Print total rows processed
    print("Total rows processed for ", filepath, ": ", total_rows)
        
        
    


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath = 'data/song_data', func = process_song_file)
    process_data(cur, conn, filepath = 'data/log_data', func = process_log_file)

    conn.close()
    
    print("ETL executed successfully")


if __name__ == "__main__":
    main()