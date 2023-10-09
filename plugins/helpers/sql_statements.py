"""
module containing all SQL queries.
"""
class SqlQuery:
    staging_songs_table_drop  = ("""DROP TABLE IF EXISTS staging_songs""")
    staging_events_table_drop = ("""DROP TABLE IF EXISTS staging_events""")
    songplay_table_drop       = ("""DROP TABLE IF EXISTS songplay""")

    staging_songs_table_create = ("""
            CREATE TABLE staging_songs (
                    song_id VARCHAR NOT NULL,
                    artist_id VARCHAR NOT NULL,
                    artist_name VARCHAR(500) NOT NULL,
                    artist_location VARCHAR(500) NOT NULL,
                    artist_latitude DECIMAL,
                    artist_longitude DECIMAL,
                    num_songs INT NOT NULL,
                    title VARCHAR,
                    duration DECIMAL NOT NULL,
                    year INT NOT NULL
                )""")
    
    staging_events_table_create = ("""
        CREATE TABLE staging_events (
                artist VARCHAR,
                auth VARCHAR(15),
                firstName VARCHAR(50),
                gender CHAR,
                itemInSession SMALLINT NULL,
                lastName VARCHAR(50),
                length DECIMAL,
                level VARCHAR(10),
                location VARCHAR,
                method VARCHAR(10),
                page VARCHAR(20),
                registration DECIMAL,
                sessionId INT NULL,
                song VARCHAR,
                status SMALLINT,
                ts BIGINT,
                userAgent VARCHAR,
                user_id INT      
            )""")

    songplay_table_create = ("""
         CREATE TABLE songplay(
            songplay_id VARCHAR PRIMARY KEY,
            start_time TIMESTAMP,
            user_id INT,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INT,
            location VARCHAR(500),
            user_agent VARCHAR(500)
         ) 
    """)

    user_table_create = ("""
      CREATE TABLE IF NOT EXISTS "user" (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        level varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
      )""")

    time_table_create = ("""
      CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL,
        "hour" int4,
        "day" int4,
        week int4,
        "month" varchar(256),
        "year" int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
      )""")

    song_table_create = ("""
      CREATE TABLE IF NOT EXISTS song (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
      )""")

    artist_table_create = ("""
      CREATE TABLE IF NOT EXISTS artist (
        artistid VARCHAR NOT NULL,
        name VARCHAR(500),
        location VARCHAR(500),
        lattitude DECIMAL,
        longitude DECIMAL
    )""")


    songplay_table_insert = ("""
        INSERT INTO songplay(songplay_id, start_time, user_id, level,
                        song_id, artist_id, session_id, location, user_agent)
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.user_id, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO "user"(userid, first_name, last_name, gender, level)
        SELECT distinct user_id, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO song(songid, title, artistid, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artist(artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplay
    """)