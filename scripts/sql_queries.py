import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS events_staging (
    artist VARCHAR(500),
    auth VARCHAR(25),
    firstName VARCHAR(50),
    gender VARCHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR(4),
    location VARCHAR(500),
    method VARCHAR(4),
    page VARCHAR(25),
    registration BIGINT,
    sessionId INTEGER,
    song VARCHAR(350),
    status INTEGER,
    ts BIGINT NOT NULL,
    userAgent VARCHAR(500),
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging (
    artist_id VARCHAR(18) NOT NULL,
    artist_latitude FLOAT,
    artist_location VARCHAR(500),
    artist_longitude FLOAT,
    artist_name VARCHAR(500) NOT NULL,
    duration FLOAT NOT NULL,
    num_songs INTEGER,
    song_id VARCHAR(18) NOT NULL,
    title VARCHAR(350) NOT NULL,
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER GENERATED ALWAYS AS IDENTITY,
    start_time TIMESTAMP NOT NULL sortkey,
    user_id INTEGER NOT NULL,
    level VARCHAR(4),
    song_id VARCHAR(18),
    artist_id VARCHAR(18),
    session_id INTEGER,
    location VARCHAR(500),
    user_agent VARCHAR(500),
    PRIMARY KEY (songplay_id)
) distkey(song_id, artist_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL sortkey,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(1),
    level VARCHAR(4) NOT NULL,
    PRIMARY KEY (user_id)
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(18) NOT NULL sortkey distkey,
    title VARCHAR(250) NOT NULL,
    artist_id VARCHAR(18) NOT NULL,
    year INTEGER sortkey,
    duration FLOAT NOT NULL,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(18) NOT NULL sortkey distkey,
    name VARCHAR(500) NOT NULL,
    location VARCHAR(500),
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL sortkey,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    PRIMARY KEY (start_time)
);
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY events_staging FROM '{config['S3']['LOG_DATA']}' 
CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}' 
JSON '{config['S3']['log_jsonpath']}'
REGION '{config['CLUSTER']['REGION']}'
""")

staging_songs_copy = (f"""
COPY songs_staging FROM '{config['S3']['SONG_DATA']}' 
CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}' 
JSON 'auto'
REGION '{config['CLUSTER']['REGION']}'
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time,
    userId AS user_id,
    level,
    s.song_id,
    s.artist_id,
    sessionId AS session_id,
    location,
    userAgent AS user_agent
FROM events_staging ev
LEFT JOIN songs_staging s ON s.title = ev.song AND s.artist_name = ev.artist AND s.duration = ev.length
WHERE userId IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    e.userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM events_staging e
JOIN (SELECT userId, MAX(ts) updated FROM events_staging GROUP BY userId) recent ON recent.userId = e.userId and recent.updated = e.ts
WHERE e.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM songs_staging;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    s.artist_id,
    MAX(artist_name) AS name,
    MAX(artist_location) AS location,
    MAX(artist_latitude) AS latitude,
    MAX(artist_longitude) AS longitude
FROM songs_staging s
JOIN (SELECT artist_id, MAX(year) AS year FROM songs_staging GROUP BY artist_id) recent ON recent.artist_id = s.artist_id and recent.year = s.year
GROUP BY s.artist_id;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time,
    EXTRACT(HR FROM start_time) AS hour,
    EXTRACT(D FROM start_time) AS day,
    EXTRACT(W FROM start_time) AS week,
    EXTRACT(MON FROM start_time) AS month,
    EXTRACT(Y FROM start_time) AS year,
    EXTRACT(DOW FROM start_time) AS weekday
FROM (SELECT userId, TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time FROM events_staging) t
WHERE userId IS NOT NULL;
""")

# QUERY LISTS

create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [
    songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create
]
drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert
]
