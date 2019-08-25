import configparser

cfg = configparser.ConfigParser()
cfg.read_file(open('dwh.cfg'))

ARN = cfg.get('IAM_ROLE','ARN')
LOG_DATA = cfg.get('S3','LOG_DATA')
LOG_JSONPATH = cfg.get('S3','LOG_JSONPATH')
SONG_DATA = cfg.get('S3','SONG_DATA')

# Drop Tables
drop_song_data_table = "drop table if exists stage_song_data"
drop_log_data_table = "drop table if exists stage_log_data"

drop_songplays_fact = "drop table if exists songplays"
drop_users_dim = "drop table if exists users"
drop_songs_dim = "drop table if exists songs"
drop_artists_dim = "drop table if exists artists"
drop_time_dim = "drop table if exists time"


# Create Stage Table
create_song_data_table = "create table stage_song_data\
							(artist_id varchar(128), \
							artist_latitude DOUBLE PRECISION,\
							artist_location varchar(512), \
							artist_longitude DOUBLE PRECISION,\
							artist_name varchar(256),\
							duration DOUBLE PRECISION,\
							num_songs integer, \
							song_id varchar(256), \
							title varchar(512), \
							year integer\
							)"
create_log_data_table = "create table stage_log_data \
							(artist varchar(128), \
							auth varchar(128), \
							firstName varchar(32), \
							gender char(1), \
							itemInSession integer, \
							lastName varchar(256), \
							length DOUBLE PRECISION, \
							level varchar(128), \
							location varchar(512), \
							method varchar(128), \
							page varchar(128),\
							registration DOUBLE PRECISION, \
							sessionId integer, \
							song varchar(256), \
							status integer, \
							ts BIGINT, \
							userAgent varchar(512), \
							userId varchar(24)\
							)"

# Create Fact and Dimension Tables
create_songplays_fact_table = "create table songplays \
								(songplay_id integer IDENTITY(0,1), \
								start_time timestamp sortkey,\
								user_id varchar(16), \
								level varchar(128), \
								song_id varchar(256) distkey, \
								artist_id varchar(128), \
								session_id integer, \
								location varchar(512), \
								user_agent varchar(512), \
								primary key(songplay_id)\
								)"
create_users_dim_table = "create table users \
							(user_id varchar(16), \
							first_name varchar(256), \
							last_name varchar(256), \
							gender char(1),  \
							level varchar(128), \
							primary key(user_id)\
							) diststyle all"
create_songs_dim_table = "create table songs\
							(song_id varchar(256) distkey, \
							title varchar(512), \
							artist_id varchar(128), \
							year integer sortkey, \
							duration DOUBLE PRECISION, \
							primary key(song_id)\
							)"
create_artists_dim_table="create table artists \
							(artist_id varchar(128), \
							name varchar(256), \
							location varchar(512), \
							lattitude DOUBLE PRECISION, \
							longitude DOUBLE PRECISION, \
							primary key(artist_id)\
							)diststyle all"
create_time_dim_table = "create table time \
							(start_time timestamp, \
							hour integer, \
							day integer, \
							week integer, \
							month integer, \
							year integer, \
							weekday integer, \
							primary key (start_time)\
							) diststyle all"

# COPY commands
copy_stage_song_data = ("""COPY stage_song_data FROM {} iam_role {} format as json 'auto'""").format(SONG_DATA,ARN)
copy_stage_log_data = ("""COPY stage_log_data FROM {} iam_role {} json {}""").format(LOG_DATA,ARN,LOG_JSONPATH)

# INSERT commands
insert_songplay_fact = "insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
	select timestamp 'epoch' + CAST(sl.ts AS BIGINT)/1000 * interval '1 second' as start_time, sl.userid as user_id, sl.level, sd.song_id, sd.artist_id, sl.sessionid as session_id, sl.location, sl.userAgent as user_agent\
	from stage_log_data sl, stage_song_data sd \
	where sd.artist_name = sl.artist and sl.page = 'NextSong'"
insert_users_dim = "insert into users (user_id, first_name, last_name, gender, level) \
	select distinct sl.userId as user_id, sl.firstName as first_name, sl.lastName as last_name, sl.gender, sl.level from stage_log_data sl where userId is not null"	
insert_songs_dim = "insert into songs (song_id, title, artist_id, year, duration) \
	select distinct sd.song_id, sd.title, sd.artist_id, sd.year, sd.duration from stage_song_data sd"
insert_artist_dim = "insert into artists (artist_id, name, location, lattitude, longitude) \
	select distinct sd.artist_id, sd.artist_name as name, sd.artist_location as location, sd.artist_latitude as lattitude, sd.artist_longitude as longitude\
	 from stage_song_data sd"
insert_time_dim = "insert into time (start_time, hour, day, week, month, year, weekday) \
	select distinct trunc(start_time), extract(hour from start_time) as hour, extract(day from start_time) as day, extract(week from start_time) as week, \
	 extract(month from start_time) as month, extract(year from start_time) as year, extract(weekday from start_time) as weekday \
	 from songplays"

drop_stage_tables = [drop_song_data_table, drop_log_data_table]
create_stage_tables = [create_song_data_table, create_log_data_table]
drop_main_tables = [drop_time_dim,drop_artists_dim,drop_songs_dim, drop_users_dim, drop_songplays_fact]
create_main_tables=[create_songplays_fact_table, create_users_dim_table, create_songs_dim_table, create_artists_dim_table, create_time_dim_table]
copy_stage_tables = [copy_stage_log_data,copy_stage_song_data]
insert_tables = [insert_songplay_fact,insert_users_dim,insert_songs_dim,insert_artist_dim,insert_time_dim]