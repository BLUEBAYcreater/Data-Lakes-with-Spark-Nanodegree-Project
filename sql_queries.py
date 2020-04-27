songs_table_query = "SELECT distinct song_id, title as song_title, artist_id, year, duration FROM songs"

artists_table_query = "SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs"

log_filtered_query = "SELECT *, cast(ts/1000 as Timestamp) as timestamp from staging_events where page = 'NextSong'"

users_query = ("""
select a.userId, a.firstName, a.lastName, a.gender, a.level
from staging_events a
inner join (
select userId, max(ts) as ts 
from staging_events 
group by userId, page
) b on a.userId = b.userId and a.ts = b.ts
""")

time_query = ("""
select distinct timestamp as start_time, 
hour(timestamp) as hour, 
day(timestamp) as day, 
weekofyear(timestamp) as week, 
month(timestamp) as month, 
year(timestamp) as year, 
weekday(timestamp) as weekday
from staging_events
""")

songplays_query = ("""
select a.timestamp as start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location, a.userAgent, year(a.timestamp) as year, month(a.timestamp) as month 
from staging_events as a 
inner join songs as b on a.song = b.song_title
""")