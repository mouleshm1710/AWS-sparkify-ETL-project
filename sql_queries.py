# Importing data warehouse credentials
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
SESSION_TOKEN = config.get('AWS', 'SESSION_TOKEN')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
DWH_ROLE_ARN = config.get("DWH", "DWH_ROLE_ARN")
DWH_VPC_ID = config.get("DWH", "DWH_VPC_ID")

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging tables
staging_songs_table_create = (
    """CREATE TABLE stagingsongs (
        num_songs INT,
        artist_id VARCHAR(255),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        song_id VARCHAR(255),
        title VARCHAR(255),
        duration FLOAT,
        year INT
    )"""
)

staging_events_table_create = (
    """CREATE TABLE stagingevents (
        artist VARCHAR(255),
        auth VARCHAR(255),
        firstName VARCHAR(255),
        gender CHAR(1),
        iteminsession INT,
        lastName VARCHAR(255),
        length FLOAT,
        level VARCHAR(255),
        location VARCHAR(255),
        method VARCHAR(255),
        page VARCHAR(255),
        registration BIGINT,
        sessionid BIGINT,
        song VARCHAR(255),
        status BIGINT,
        ts BIGINT,
        useragent TEXT,
        userid INT
    );"""
)

# Analytical Tables
# Columns - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = (
    """CREATE TABLE songplays (
        songplayid BIGINT IDENTITY(0, 1) PRIMARY KEY,
        starttime TIMESTAMP,
        userid INT,
        level VARCHAR(255),
        songid VARCHAR(255),
        artistid VARCHAR(255),
        sessionid BIGINT,
        location VARCHAR(255),
        useragent TEXT
    );"""
)

# Columns - user_id, first_name, last_name, gender, level
user_table_create = (
    """CREATE TABLE users (
        userid INT PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        gender CHAR(1),
        level VARCHAR(255)
    );"""
)

# Columns - song_id, title, artist_id, year, duration
song_table_create = (
    """CREATE TABLE songs (
        song_id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        artist_id VARCHAR(255),
        year INT,
        duration FLOAT
    );"""
)

# Columns - artist_id, name, location, latitude, longitude
artist_table_create = (
    """CREATE TABLE artists (
        artist_id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        location VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT
    );"""
)

# Columns - start_time, hour, day, week, month, year, weekday
time_table_create = (
    """CREATE TABLE time (
        starttime TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );"""
)

# COPY FROM S3 -> STAGING TABLES
staging_events_copy = """
COPY stagingevents 
FROM 's3://udacity-dend/log_data/' 
CREDENTIALS 'aws_iam_role={}' 
JSON 's3://udacity-dend/log_json_path.json' 
COMPUPDATE OFF 
TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL 
REGION 'us-west-2';
""".format(DWH_ROLE_ARN)

staging_songs_copy = """
COPY stagingsongs 
FROM 's3://udacity-dend/song_data/' 
CREDENTIALS 'aws_iam_role={}' 
JSON 'auto' 
COMPUPDATE OFF 
TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL 
REGION 'us-west-2';
""".format(DWH_ROLE_ARN)

# FINAL ANALYTICAL TABLES (INSERT STATEMENTS)

songplay_table_insert = (
    """
    INSERT INTO songplays (starttime, userid, level, songid, artistid, sessionid, location, useragent)
    SELECT 
        TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS starttime, 
        e.userid AS userid, 
        e.level AS level, 
        s.song_id AS songid, 
        s.artist_id AS artistid, 
        e.sessionid AS sessionid, 
        e.location AS location, 
        e.useragent AS useragent 
    FROM stagingevents e 
    JOIN stagingsongs s ON e.song = s.title 
        AND e.artist = s.artist_name 
        AND e.length = s.duration 
    WHERE e.page = 'NextSong';
    """
)

user_table_insert = (
    """
    INSERT INTO users (userid, firstname, lastname, gender, level)
    SELECT DISTINCT 
        userid, 
        firstname, 
        lastname, 
        gender, 
        level 
    FROM stagingevents 
    WHERE userid IS NOT NULL;
    """
)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id, 
        title, 
        artist_id,
        year, 
        duration 
    FROM stagingsongs 
    WHERE song_id IS NOT NULL;
    """
)

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS longitude 
    FROM stagingsongs 
    WHERE artist_id IS NOT NULL;
    """
)

time_table_insert = (
    """
    INSERT INTO time (starttime, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS starttime, 
        EXTRACT(hour FROM (TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second')) AS hour, 
        EXTRACT(day FROM (TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second')) AS day, 
        EXTRACT(week FROM (TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second')) AS week, 
        EXTRACT(month FROM (TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second')) AS month, 
        EXTRACT(year FROM (TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second')) AS year, 
        EXTRACT(weekday FROM (TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second')) AS weekday 
    FROM stagingevents 
    WHERE ts IS NOT NULL 
        AND page = 'NextSong';
    """
)

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]

#### END ####
