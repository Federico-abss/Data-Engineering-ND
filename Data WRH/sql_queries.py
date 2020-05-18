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

custom_types_create = ("CREATE TYPE premium AS ENUM ('paid', 'free'); CREATE TYPE sex AS ENUM ('M', 'F');")

staging_events_table_create= ("""CREATE TABLE staging_events
(artist varchar,
auth varchar,
firstName varchar,
gender sex,
itemInSession int,
lastName varchar,
length float,
level premium,
location varchar,
method varchar,
page varchar,
registration float,
sessionId int,
song varchar,
status int,
ts TIMESTAMP,
userAgent varchar,
userId int);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs
(num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year int);
""")

songplay_table_create = ("""CREATE TABLE songplays 
(songplay_id int IDENTITY(0,1) PRIMARY KEY, 
start_time date, 
user_id int NOT NULL, 
level premium, 
song_id text, 
artist_id text, 
session_id int, 
location text, 
user_agent text);
""")

user_table_create = ("""CREATE TABLE users 
(user_id int PRIMARY KEY, 
first_name text, 
last_name text,
gender sex,
level premium);
""")

song_table_create = ("""CREATE TABLE songs 
(song_id text PRIMARY KEY, 
title text, 
artist_id text NOT NULL, 
year int, 
duration float NOT NULL);
""")

artist_table_create = ("""CREATE TABLE artists 
(artist_id text PRIMARY KEY, 
name text, 
location text, 
latitude text, 
longitude text);
""")

time_table_create = ("""CREATE TABLE time 
(start_time date PRIMARY KEY, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday text);
""")


# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

staging_events_table_clean = ("DELETE FROM staging_events WHERE page != 'NextSong';")

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  DISTINCT(e.ts) AS start_time, 
e.userId AS user_id, 
e.level AS level, 
s.song_id AS song_id, 
s.artist_id AS artist_id, 
e.sessionId AS session_id, 
e.location AS location, 
e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT  DISTINCT(userId) AS user_id,
firstName AS first_name,
lastName AS last_name,
gender,
level
FROM staging_events
WHERE user_id IS NOT NULL;
ON CONFLICT (user_id) DO NOTHING;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT  DISTINCT(song_id) AS song_id,
title,
artist_id,
year,
duration
FROM staging_songs
WHERE song_id IS NOT NULL
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT  DISTINCT(artist_id) AS artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS latitude,
artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(start_time) AS start_time,
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(week FROM start_time) AS week,
EXTRACT(month FROM start_time) AS month,
EXTRACT(year FROM start_time) AS year,
EXTRACT(dayofweek FROM start_time) as weekday
FROM songplays;
ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [custom_types_create, staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [staging_events_table_clean, artist_table_insert, user_table_insert, song_table_insert, time_table_insert]
