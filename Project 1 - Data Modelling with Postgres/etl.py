import os
import glob
import psycopg2
import pandas as pd
import sys
from sql_queries import *

# Procedure to process the song file and load the records into Database
def process_song_file(cur, filepath):
    """
        Procedure to process the song file and load the records into the database.
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    try:
        # insert song record
        song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
        cur.execute(song_table_insert, song_data)
    except Exception as e:
        print("Error while inserting into song table - " )
        print(e)
        sys.exit(0)
    
    try:
        # insert artist record
        artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print("Error while inserting into artist table - " )
        print(e)
        sys.exit(0)

# Procedure to process the log file and load the records into the database.
def process_log_file(cur, filepath):
    """
        Procedure to process the log file and load the records into the database
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts)
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('ts','hr','dy','wk','mn','yr','wd')
    val = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame(val)

    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    except Exception as e:
        print("Error while inserting into time table - " )
        print(e)
        sys.exit(0)

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    try:
        # insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    except Exception as e:
        print("Error while inserting into user table - " )
        print(e)
        sys.exit(0)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

    try:
        # insert songplay record
        songplay_data = ( pd.to_datetime(row['ts']),row['userId'],row['level'], songid, artistid, row['sessionId'],row['location'],row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)
    except Exception as e:
        print("Error while inserting into time table - " )
        print(e)
        sys.exit(0)

# Wrapper procedure to process song/log data into the database based on the parameter value.
def process_data(cur, conn, filepath, func):
    """
        Wrapper procedure to process song/log data into the database based on the parameter value.
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()