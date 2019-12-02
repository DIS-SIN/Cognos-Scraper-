"""Push processed extract to cloud DB."""
import logging
from departments_scraper.config.directories import PROCESSED_DIR
from utils.db import get_db, run_mysql

# Instantiate logger
logger = logging.getLogger(__name__)

# Store DB connection in global var to avoid reconnecting after each query
cnx = get_db()
logger.debug('1/7: Connected to DB.')

# MySQL requires paths with forward slashes
PROCESSED_DIR = PROCESSED_DIR.replace('\\', '/')

create_table = """
	CREATE TABLE new_departments(
		dept_code VARCHAR(75) PRIMARY KEY,
		dept_name_en VARCHAR(120),
		dept_name_fr VARCHAR(120)
	);
"""

load_data = """
	LOAD DATA LOCAL INFILE '{0}/departments_processed.csv'
	INTO TABLE new_departments
	FIELDS OPTIONALLY ENCLOSED BY '"'
	TERMINATED BY ','
	LINES TERMINATED BY '\r\n'
	IGNORE 1 LINES;
""".format(PROCESSED_DIR)

indices = []

# Rename tables in a single atomic transaction
# Ensures clean switchover + no downtime
rename_tables = """
	RENAME TABLE departments TO old_departments, new_departments TO departments;
"""

drop_table = """
	DROP TABLE old_departments;
"""

try:
	run_mysql(cnx, create_table)
	logger.debug('2/7: New table created.')
	run_mysql(cnx, load_data)
	logger.debug('3/7: Data loaded.')
	for index in indices:
		run_mysql(cnx, index)
	logger.debug('4/7: Indices created.')
	run_mysql(cnx, rename_tables)
	logger.debug('5/7: Tables renamed.')
	run_mysql(cnx, drop_table)
	logger.debug('6/7: Old table dropped.')
except Exception as e:
	logger.critical('Failure!', exc_info=True)
	cnx.close()
	exit()
finally:
	cnx.close()
	logger.debug('7/7: Connection closed.')
