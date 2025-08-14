import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    Opens a JSON file of song info and inserts data into a dataframe
    
    Assigns cell data from dataframe into the song_data variable.
    Uses psycopg2 cursor object to exceute the SQL function from sql_queries to insert song_data into the "songs" table.
    
    Assigns cell data from dataframe into the artist_data variable.
    Uses psycopg2 cursor object to exceute the SQL function from sql_queries to insert artist_data into the "artists" table.
    
    Parameters
    ----------
    cur : cursor
        cursor of psycopg2 database connection
    filepath : file
        song data file
    """
    
    # open song file
    df = pd.read_json(filepath, dtype={'year': int}, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    Opens a JSON file of log info and inserts data into a dataframe
    
    Filters the dataframe by the value in the "page" column
    
    Converts the datatype in the "ts" column into datetime
    
    Assigns cell data from dataframe into the time_data variable.
    Iterates throughout each row in the "time_df" dataframe. Uses the psycopg2 cursor object to exceute the SQL function from 
    sql_queries to insert time_data into the "time" table.
    
    Assigns cell data from dataframe into the user_df dataframe variable.
    Iterates throughout each row in the "user_df" dataframe. Uses the psycopg2 cursor object to exceute the SQL function from 
    sql_queries to insert _data into the "users" table.
    
    Iterates throughout each row in the "df" dataframe.
    Uses the psycopg2 cursor object to exceute the SQL function from sql_queries to insert _data into the "songplays" table.
    
    Parameters
    ----------
    cur : cursor
        cursor of psycopg2 database connection
    filepath : file
        log data file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].copy()

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # convert dataframe timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit='ms')
    
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
        songplay_data = [index+1, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Load song_data and log_data files and execute respective function with each file. 
    Return the number of files found in each directory and ther number of files processed.
    
    Parameters
    ----------
    cur : cursor
        cursor of psycopg2 database connection.
    conn : connection
        connection of psycopg2
    filepath : string
        directory of files
    func : def
        function for processing of each file
        
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

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    Connect to sparkifydb database, create Cursor object, and run the process_data() functions for song_data and log_data
    
    Parameters
    ----------
    None
    """
    
    # connect to default database
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect("host=localhost port=5433 dbname=sparkifydb user=postgres password=w8t3r1")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    cur.close() # Line added to close any connections to the database
    conn.close()


if __name__ == "__main__":
    main()