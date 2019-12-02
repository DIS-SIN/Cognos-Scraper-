import os
import mysql.connector

def get_db():
	"""Connect to remote MySQL DB."""
	import logging
	# Instantiate logger
	logger = logging.getLogger(__name__)
	# Check if credentials stored in environ vars
	HOST = os.environ.get('DB_HOST', None)
	USER = os.environ.get('DB_USER', None)
	PASSWORD = os.environ.get('DB_PASSWORD', None)
	DATABASE = os.environ.get('DB_DATABASE_NAME', None)
	if HOST is None or USER is None or PASSWORD is None or DATABASE is None:
		logger.critical('Failure: Missing database credentials.')
		exit()
	db = mysql.connector.connect(host=HOST,
								 user=USER,
								 password=PASSWORD,
								 database=DATABASE)
	return db


def run_mysql(cnx, query, args=None):
	"""Run commands on remote MySQL DB."""
	cursor = cnx.cursor()
	cursor.execute(query, args)
	cnx.commit()
	cursor.close()


def query_mysql(cnx, query, args=None, dict_=False):
	"""Run query and return results."""
	cursor = cnx.cursor(dictionary=dict_)
	cursor.execute(query, args)
	results = cursor.fetchall()
	cursor.close()
	return results
