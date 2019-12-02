"""Ensure freshly downloaded file(s) has row count(s) greater than
or equal to current DB row count(s). This is to ensure corrupted files
(known issue in Cognos) aren't pushed to DB.
"""
import logging
import os
import pandas as pd
from config import shared_directories
from utils.db import get_db, query_mysql

# Instantiate logger
logger = logging.getLogger(__name__)

# Store DB connection in global var to avoid reconnecting after each query
cnx = get_db()
logger.debug('1/5: Connected to DB.')

# Get row counts by course code from DB
# Don't filter by status and can be changed by learners at any time
course_code_query = """
	SELECT course_code, COUNT(reg_id)
	FROM lsr_this_year
	GROUP BY course_code;
"""

try:
	course_code_count_db = query_mysql(cnx, course_code_query)
	logger.debug('2/5: Queried DB for row counts.')
except Exception:
	logger.critical('Failure!', exc_info=True)
	cnx.close()
	exit()
finally:
	cnx.close()
	logger.debug('3/5: Connection closed.')

# Get total row count from Pandas
os.chdir(shared_directories.DOWNLOADS_DIR)
regs = pd.read_csv('Registrations.csv', sep='\t', index_col=False, encoding='utf_16_le',
				   keep_default_na=False)

# Get row counts by course code from Pandas
course_code_count_pd = regs['course_code'].value_counts(sort=False, dropna=False)
course_code_count_pd = {tup[0].upper(): tup[1] for tup in course_code_count_pd.iteritems()}
logger.debug('4/5: Queried Pandas for row counts.')

# Compare DB counts with Pandas counts, using DB as baseline
for tup in course_code_count_db:
	course_code = tup[0].upper()
	db_count = tup[1]
	pd_count = course_code_count_pd.get(course_code, 0)
	if pd_count < db_count:
		logger.critical('Failure: Missing data in latest Cognos extract for course code {0}.'.format(course_code))
		exit()

# Log new rows this ETL
# Useful to get a feeling for data by day of week, time of year, etc.
# Also insurance against repeated days of 0 new rows -> problem with Cognos
total_db = sum([tup[1] for tup in course_code_count_db])
total_pd = regs.shape[0]
new_rows = total_pd - total_db
logger.info('{0} new registrations today.'.format(new_rows))

logger.debug('5/5: Check complete: No missing data.')
