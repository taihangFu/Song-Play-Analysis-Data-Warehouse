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
TIMEFORMAT as 'epochmillisecs'
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
## to_timestamp('timestamp','format') in redshift takes string inputs thus need uses to_char()
songplay_table_insert = ("""
INSERT INTO songplays (
songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
     SELECT DISTINCT to_timestamp(to_char(e.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS')
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';

""")

user_table_insert = ("""
    INSERT INTO users (                 user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT  DISTINCT se.userId          AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
    FROM staging_events AS se
    WHERE se.userid IS NOT NULL AND se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (                 song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM staging_songs AS ss;
    WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (               artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs AS ss
    WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT DISTINCT ts,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.ts IS NOT NULL AND se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
