# Data-Modeling with Postgres
--------------------------------

The purpose of this project is to implement a dimension data model for analysis of user song-play pattern.

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