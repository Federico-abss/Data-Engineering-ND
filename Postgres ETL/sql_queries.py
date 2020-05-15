# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

custom_types_create = ("CREATE TYPE premium AS ENUM ('paid', 'free'); CREATE TYPE sex AS ENUM ('M', 'F');")

songplay_table_create = ("""CREATE TABLE songplays 
(songplay_id int PRIMARY KEY, 
start_time date REFERENCES time(start_time), 
user_id int NOT NULL REFERENCES users(user_id), 
level premium, 
song_id text REFERENCES songs(song_id), 
artist_id text REFERENCES artists(artist_id), 
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
artist_id text NOT NULL REFERENCES artists(artist_id), 
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

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""SELECT song_id, songs.artist_id 
FROM songs JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s
AND artists.name = %s
AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [custom_types_create, user_table_create,  artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]