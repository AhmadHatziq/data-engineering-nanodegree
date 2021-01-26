import configparser

"""
    This file contains the SQL statements which
    will be used by etl.py and create_tables.py.
"""

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """
DROP TABLE IF EXISTS staging_schema.staging_events ;
"""

staging_songs_table_drop = """
DROP TABLE IF EXISTS staging_schema.staging_songs ;
"""

songplay_table_drop = """
DROP TABLE IF EXISTS star_schema.songplays ;
"""

user_table_drop = """
DROP TABLE IF EXISTS star_schema.users ;
"""

song_table_drop = """
DROP TABLE IF EXISTS star_schema.songs ;
"""

time_table_drop = """
DROP TABLE IF EXISTS star_schema.time
"""

artist_table_drop = """
DROP TABLE IF EXISTS star_schema.artists ;
"""

staging_schema_drop = """
DROP SCHEMA IF EXISTS staging_schema CASCADE;
"""

star_schema_drop = """
DROP SCHEMA IF EXISTS star_schema CASCADE"""

# SCHEMA statements
staging_schema_create = ("""
CREATE SCHEMA IF NOT EXISTS staging_schema;
""")

star_schema_create = ("""
CREATE SCHEMA IF NOT EXISTS star_schema;
""")

# CREATE STAGING TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_schema.staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration DECIMAL,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_schema.staging_songs
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL,
    year DECIMAL
);
""")

# CREATE FACT TABLE

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS star_schema.songplays
(
    songplay_id INTEGER IDENTITY (1, 1) PRIMARY KEY ,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
)
DISTKEY ( start_time )
SORTKEY ( start_time );
""")

# CREATE DIMENSION TABLE

user_table_create = ("""
CREATE TABLE IF NOT EXISTS star_schema.users
(
    userId INTEGER PRIMARY KEY,
    firsname VARCHAR,
    lastname VARCHAR,
    gender VARCHAR(2) ,
    level VARCHAR
)
SORTKEY (userId);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS star_schema.songs
(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration DECIMAL
)
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS star_schema.artists
(
    artist_id VARCHAR PRIMARY KEY ,
    name VARCHAR,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL
)
SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS star_schema.time
(
    start_time TIMESTAMP PRIMARY KEY ,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    year INTEGER,
    month INTEGER,
    weekday VARCHAR(10) 
)
DISTKEY ( start_time )
SORTKEY (start_time);
""")

# STAGING TABLES COPY

staging_events_copy = ("""
COPY staging_schema.staging_events
FROM {}
credentials 'aws_iam_role={}'
FORMAT AS json {}
region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['DWH']['dwh_role_arn'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_schema.staging_songs
FROM {}
credentials 'aws_iam_role={}'
FORMAT AS json 'auto'
region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('DWH','dwh_role_arn'))

# FINAL TABLES INSERTION

time_table_insert = ("""
-- Import data from staging_events -> time
-- Conversion from ms to TIMESTAMP taken from:
-- https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
INSERT INTO star_schema.time
SELECT 
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_schema.staging_events
""")

user_table_insert = ("""
INSERT INTO star_schema.users
SELECT userId, firstName, lastName, gender, level
FROM staging_schema.staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert =  ("""
INSERT INTO star_schema.songs
SELECT song_id, title, artist_id, year, duration
FROM staging_schema.staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO star_schema.artists
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_schema.staging_songs;
""")

songplay_table_insert = ("""
-- Time code taken from: https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
INSERT INTO star_schema.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_schema.staging_songs ss
INNER JOIN staging_schema.staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [time_table_insert, user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert]

create_schema_queries = [staging_schema_create, star_schema_create]

drop_schema_queries = [staging_schema_drop, star_schema_drop]