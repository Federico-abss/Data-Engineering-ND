songs_table_query = "SELECT song_id, title, artist_id, year, duration FROM songs"

artists_table_query = "SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude as longitude FROM songs"

log_filtered_query = "SELECT *, cast(ts/1000 as Timestamp) AS timestamp FROM staging_events WHERE page = 'NextSong'"

users_query = ("""
SELECT a.userId, a.firstName, a.lastName, a.gender, a.level
FROM staging_events a
INNER JOIN (
select userId, max(ts) AS ts 
FROM staging_events 
GROUP BY userId, page
) b ON a.userId = b.userId AND a.ts = b.ts
""")

time_query = ("""
SELECT timestamp AS start_time, 
hour(timestamp) AS hour, 
day(timestamp) AS day, 
weekofyear(timestamp) AS week, 
month(timestamp) AS month, 
year(timestamp) AS year, 
weekday(timestamp) AS weekday
FROM staging_events
""")

songplays_query = ("""
SELECT a.timestamp AS start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location, a.userAgent, year(a.timestamp) AS year, month(a.timestamp) AS month 
FROM staging_events a 
INNER JOIN songs b ON a.song = b.title
""")