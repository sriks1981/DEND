# Data-Modeling with Redshift

------

The purpose of this project is to implement a dimension data model for analysis of user song-play pattern.

## Database Design

------

The database design consists of 5 tables in total, 1 fact and 4 dimension tables. The structure of the these tables are as follows -

### Fact Table

------

#### SONGPLAYS

| Column      | Data Type                         |
| ----------- | --------------------------------- |
| SONGPLAY_ID | INTEGER IDENTITY(0,1) PRIMARY KEY |
| START_TIME  | TIMESTAMP                         |
| USER_ID     | VARCHAR(16)                       |
| LEVEL       | VARCHAR(128)                      |
| SONG_ID     | VARCHAR(256)                      |
| ARTIST_ID   | VARCHAR(128)                      |
| SESSION_ID  | NUMBER                            |
| LOCATION    | VARCHAR(512)                      |
| USER_AGENT  | VARCHAR(512)                      |

### Dimension Table(s)

------

#### USERS

| Column     | Data Type               |
| ---------- | ----------------------- |
| USER_ID    | VARCHAR(16) PRIMARY KEY |
| FIRST_NAME | VARCHAR(256)            |
| LAST_NAME  | VARCHAR(256)            |
| GENDER     | CHAR(1)                 |
| LEVEL      | VARCHAR(128)            |

#### SONGS

| Column    | Data Type                |
| --------- | ------------------------ |
| SONG_ID   | VARCHAR(256) PRIMARY KEY |
| TITLE     | VARCHAR(512)             |
| ARTIST_ID | VARCHAR(128)             |
| YEAR      | NUMBER                   |
| DURATION  | DOUBLE PRECISION         |

#### ARTISTS

| Column    | Data Type                |
| --------- | ------------------------ |
| ARTIST_ID | VARCHAR(128) PRIMARY KEY |
| NAME      | VARCHAR(256)             |
| LOCATION  | VARCHAR(512)             |
| LATITUDE  | DOUBLE PRECISION         |
| LONGITUDE | DOUBLE PRECISION         |

#### TIME

| Column     | Data Type             |
| ---------- | --------------------- |
| START_TIME | TIMESTAMP PRIMARY KEY |
| HOUR       | NUMBER                |
| DAY        | NUMBER                |
| WEEK       | NUMBER                |
| MONTH      | NUMBER                |
| YEAR       | NUMBER                |
| WEEKDAY    | NUMBER                |

## Code Repository

------

1. **create_tables.py** - A python script to re-create the tables during every run. This should be run before  running etl.py.
2. **etl.py** - A python script which reads and processes files from song_data and log_data and loads them into the tables.
3. **sql_queries.py** - A python script which contains all the sql queries, which includes queries to drop, create, insert and select data.
4. **README.md** - A brief writeup of the project implementation.
5. **dwh.cfg** - This is the configuration file which includes the details of redshift cluster for connectivity and the data sets.

## Executing code

------

The code is executed in the following sequence -

1. **create_tables.py** - This will re-create all the tables in the configured database.
2. **etl.py** - This will read all the song and log data files and load the data into the database fact/dimension tables.