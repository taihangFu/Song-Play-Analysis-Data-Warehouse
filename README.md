# Song-Play-Analysis-Data-Warehouse

## Task
Building an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team to continue finding insights in what songs users are listening to. We'll be able to test the database and ETL pipeline by running queries.

## Origin Dataset
### Song Dataset
This is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations.

The log files in the dataset we'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

## Star Schema for Song Play Analysis after ETL process
Using the song and log datasets, we'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page `NextSong`
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
2. <b>users</b> - users in the app
    * user_id, first_name, last_name, gender, level
3. <b>songs</b> - songs in music database
    * song_id, title, artist_id, year, duration
4. <b>artists</b> - artists in music database
    * artist_id, name, location, lattitude, longitude
5. <b>time</b> - timestamps of records in <b>songplays</b> broken  down into specific units
    * start_time, hour, day, week, month, year, weekday
    
## Instructions
1. <b>Create Table Schemas:</b> Run `create_tables.py` in your terminal to drop any existing tables and create new tables with the correct column data types and conditions.

2. <b>Build ETL Pipeline:</b> Run `etl.py` after running `create_tables.py` and running the analytic queries on your Redshift database to compare your results with the expected results. It will execute the COPY and INSERT SQL queries that are outlined in `sql_queries.py`. This copies the log-data and song-data from the `udacity-dend` S3 bucket, into corresponding staging tables and from there the data will be inserted into various tables that follow a star schema and are optimized for song query analysis.

3. `dwh.cfg` is a  config file, contains the AWS configuration details to connect to the redshift cluster. This has been excluded from the repo due to security and privacy purpose.
