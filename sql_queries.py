import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                (artist VARCHAR(70),
                                auth VARCHAR,
                                firstName VARCHAR(35),
                                gender VARCHAR(2),
                                itemInSession INT,
                                lastName VARCHAR(35),
                                length DOUBLE PRECISION,
                                level VARCHAR,
                                location VARCHAR,
                                method VARCHAR,
                                page VARCHAR,
                                registration DOUBLE PRECISION,
                                sessionId BIGINT,
                                song VARCHAR,
                                status INT,
                                ts TIMESTAMP,
                                userAgent VARCHAR,
                                userId VARCHAR);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (song_id VARCHAR,
                                num_songs INT,
                                title VARCHAR,
                                artist_name VARCHAR(70),
                                artist_latitude DOUBLE PRECISION,
                                year INT,
                                duration DOUBLE PRECISION,
                                artist_id VARCHAR,
                                artist_longitude DOUBLE PRECISION,
                                artist_location VARCHAR);""")

songplay_table_create = ( """CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time(start_time),
    user_id VARCHAR REFERENCES users(user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id BIGINT,
    location VARCHAR,
    user_agent TEXT)
""")


user_table_create = ("""CREATE TABLE users(
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR(35),
    last_name VARCHAR(35),
    gender VARCHAR(2),
    level VARCHAR)
""")

song_table_create = song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION)
""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR(70),
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION)
""")

time_table_create = ("""CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER)
""")

# STAGING TABLES
# Extract + Transform
staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' 
FORMAT AS JSON {}
COMPUPDATE OFF STATUPDATE OFF
BLANKSASNULL EMPTYASNULL;
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs
from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto'
COMPUPDATE OFF STATUPDATE OFF
BLANKSASNULL EMPTYASNULL;

""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
# Load
songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
