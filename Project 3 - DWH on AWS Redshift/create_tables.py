import configparser
import pandas as pd
import psycopg2
from sql_queries import *

def main():
	cfg = configparser.ConfigParser()
	cfg.read_file(open('dwh.cfg'))

	# Read from config file
	HOST = cfg.get('CLUSTER','HOST')
	DB_NAME = cfg.get('CLUSTER','DB_NAME')
	DB_USER = cfg.get('CLUSTER','DB_USER')
	DB_PASSWORD = cfg.get('CLUSTER','DB_PASSWORD')
	DB_PORT = cfg.get('CLUSTER','DB_PORT')

	ARN = cfg.get('IAM_ROLE','ARN')

	LOG_DATA = cfg.get('S3','LOG_DATA')
	LOG_JSONPATH = cfg.get('S3','LOG_JSONPATH')
	SONG_DATA = cfg.get('S3','SONG_DATA')

	# Store in Dataframes and print the values
	df = pd.DataFrame({
		"Param":['HOST','DB_NAME','DB_USER','DB_PASSWORD','DB_PORT','ARN','LOG_DATA','LOG_JSONPATH','SONG_DATA'],
		"Value":[HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT,ARN,LOG_DATA,LOG_JSONPATH,SONG_DATA]
		})

	print(df)

	# Connect to DB
	con = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DB_NAME, DB_USER,DB_PASSWORD,DB_PORT))
	cur = con.cursor()

	# Drop stage tables if they exist already
	try:
		for qry in drop_stage_tables:
			cur.execute(qry)
	except Exception as e:
		print(e)
		exit('Failed to drop stage tables')

	# Create Stage tables
	try:
		for qry in create_stage_tables:
			cur.execute(qry)
	except Exception as e:
		print(e)
		exit('Failed to create stage tables')

	# Drop main tables if they exist already
	try:
		for qry in drop_main_tables:
			cur.execute(qry)
	except Exception as e:
		print(e)
		exit('Failed to drop main tables')

	# Create main tables
	try:
		for qry in create_main_tables:
			cur.execute(qry)
	except Exception as e:
		print(e)
		exit('Failed to create main tables')

	con.commit()
	cur.close()
	con.close()


if __name__ == "__main__":
	main()