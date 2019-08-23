import configparser
import pandas as pd
import psycopg2, datetime
from sql_queries import *

# Loading Stage tables
def load_stage_tables(cur,con):
	print('Running copy to stage tables....')
	for i,qry in enumerate(copy_stage_tables):
		print('Copying to Table{}...'.format(i+1))
		cur.execute(qry)
		con.commit()
		print('Copying to Table{}...done'.format(i+1))
	print('Completed copy to stage tables')

# Loading Fact and Dimensions
def load_fact_dim_tables(cur,con):
	print('Loading into the Fact and Dimensions...')
	for i,qry in enumerate(insert_tables):
		print('Copying to Table{}...'.format(i+1))
		cur.execute(qry)
		con.commit()
		print('Copying to Table{}...done'.format(i+1))
	print('Completed loading into the Fact and Dimensions.')



def main():
	cfg = configparser.ConfigParser()
	cfg.read_file(open('dwh.cfg'))

	HOST = cfg.get('CLUSTER','HOST')
	DB_NAME = cfg.get('CLUSTER','DB_NAME')
	DB_USER = cfg.get('CLUSTER','DB_USER')
	DB_PASSWORD = cfg.get('CLUSTER','DB_PASSWORD')
	DB_PORT = cfg.get('CLUSTER','DB_PORT')

	ARN = cfg.get('IAM_ROLE','ARN')

	LOG_DATA = cfg.get('S3','LOG_DATA')
	LOG_JSONPATH = cfg.get('S3','LOG_JSONPATH')
	SONG_DATA = cfg.get('S3','SONG_DATA')

	df = pd.DataFrame({
		"Param":['HOST','DB_NAME','DB_USER','DB_PASSWORD','DB_PORT','ARN','LOG_DATA','LOG_JSONPATH','SONG_DATA'],
		"Value":[HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT,ARN,LOG_DATA,LOG_JSONPATH,SONG_DATA]
		})

	print(df)

	# Connect Redshift CLuster
	con = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DB_NAME, DB_USER,DB_PASSWORD,DB_PORT))
	cur = con.cursor()

	# Load the staging tables through COPY command
	load_stage_tables(cur,con)

	# Load the Fact and Dimension tables
	load_fact_dim_tables(cur,con)

	cur.close()
	con.close()

if __name__ == "__main__":
	main()