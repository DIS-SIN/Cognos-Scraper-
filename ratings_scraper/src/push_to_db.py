"""Push processed extract to cloud DB."""
import logging
from ratings_scraper.config.directories import PROCESSED_DIR
from utils.db import get_db, run_mysql

# Instantiate logger
logger = logging.getLogger(__name__)

# Store DB connection in global var to avoid reconnecting after each query
cnx = get_db()
logger.debug('1/7: Connected to DB.')

# MySQL requires paths with forward slashes
PROCESSED_DIR = PROCESSED_DIR.replace('\\', '/')

create_table = """
	CREATE TABLE new_ratings(
		course_code VARCHAR(20),
		survey_id VARCHAR(15),
		offering_id INT,
		fiscal_year VARCHAR(9),
		month_en VARCHAR(10),
		month_fr VARCHAR(10),
		learner_classif VARCHAR(80),
		learner_dept VARCHAR(150),
		original_question VARCHAR(75),
		numerical_answer TINYINT,
		text_answer_en VARCHAR(40),
		text_answer_fr VARCHAR(40),
		learner_dept_code VARCHAR(75),
		PRIMARY KEY(survey_id, original_question)
	);
"""

load_data = """
	LOAD DATA LOCAL INFILE '{0}/ratings_processed.csv'
	INTO TABLE new_ratings
	FIELDS OPTIONALLY ENCLOSED BY '"'
	TERMINATED BY ','
	LINES TERMINATED BY '\r\n'
	IGNORE 1 LINES
""".format(PROCESSED_DIR)

indices = [
	'CREATE INDEX idx_cc_oq_taen ON new_ratings(course_code, original_question, text_answer_en);',
	'CREATE INDEX idx_cc_oq_tafr ON new_ratings(course_code, original_question, text_answer_fr);',
	'CREATE INDEX idx_cc_fy_oq_men_na_sid ON new_ratings(course_code, fiscal_year, original_question, month_en, numerical_answer, survey_id);'
]

# Rename tables in a single atomic transaction
# Ensures clean switchover + no downtime
rename_tables = """
	RENAME TABLE ratings TO old_ratings, new_ratings TO ratings;
"""

drop_table = """
	DROP TABLE old_ratings;
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
