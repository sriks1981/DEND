# Data-Modeling with Postgres
--------------------------------

The purpose of this project is to implement a dimension data model for analysis of user song-play pattern. 

## Database Design
--------------------
The database design consists of 5 tables in total, 1 fact and 4 dimension tables. The structure of the these tables are as follows - 

### Fact Table 
------------------
#### SONGPLAYS


|Column|Data Type|
|:---------|:-----------|
|SONGPLAY_ID| SERIAL PRIMARY KEY
|START_TIME| TIMESTAMP
|USER_ID| TEXT
|LEVEL| TEXT
|SONG_ID| TEXT
|ARTIST_ID| TEXT
|SESSION_ID| NUMBER
|LOCATION| TEXT
|USER_AGENT| TEXT


### Dimension Table(s)
--------------------------

#### USERS
|Column|Data Type|
|:---------|:-----------|
|USER_ID|INT PRIMARY KEY
|FIRST_NAME|TEXT
|LAST_NAME|TEXT
|GENDER|TEXT
|LEVEL|TEXT


#### SONGS
|Column|Data Type|
|:---------|:-----------|
|SONG_ID|TEXT PRIMARY KEY
|TITLE|TEXT
|ARTIST_ID|TEXT
|YEAR|NUMBER
|DURATION|FLOAT

#### ARTISTS
|Column|Data Type|
|:---------|:-----------|
|ARTIST_ID|VARCHAR PRIMARY KEY
|NAME|TEXT|
|LOCATION|TEXT|
|LATITUDE|FLOAT|
|LONGITUDE|FLOAT|


#### TIME
|Column|Data Type|
|:---------|:-----------|
|TIME_ID|SERIAL PRIMARY KEY
|START_TIME|TIMESTAMP
|HOUR|NUMBER
|DAY|NUMBER
|WEEK|NUMBER
|MONTH|NUMBER
|YEAR|NUMBER
|WEEKDAY|NUMBER




This data model is implemented as Star Schema for 2 reasons - 

1. Each dimension table has limited attributes which can be included in a single table.
2. The data model is simple and the preference is to keep queries less complex while accessing the data patterns(limited to single join).


## Code Repository
------------------------------

1. **test.ipynb** - This is a Jupyter Notebook to validate table structure and the data in the table.
2. **create_tables.py** - A python script to re-create the tables during every run. This should be run everytime before running etl.py.
3. **etl.ipynb** - A Jupyter notebook to process a single file from song_data and log_data and loads the data into the tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. **etl.py** - A python script which reads and processes files from song_data and log_data and loads them into the tables.
5. **sql_queries.py** - A python script which contains all the sql queries, which includes queries to drop, create, insert and select data.
6. **README.md** - A brief writeup of the project implementation.

## Executing code
---------------------

The code is executed in the following sequence - 

1. **create_tables.py** - This will re-create all the tables in the sparkyifydb.
2. **etl.py** - This will read all the song and log data files and load the data into the database tables.