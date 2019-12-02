"""Push processed extract to cloud DB."""
import logging
from comments_scraper.config.directories import PROCESSED_DIR
from utils.db import get_db, run_mysql

# Instantiate logger
logger = logging.getLogger(__name__)

# Store DB connection in global var to avoid reconnecting after each query
cnx = get_db()
logger.debug('1/7: Connected to DB.')

# MySQL requires paths with forward slashes
PROCESSED_DIR = PROCESSED_DIR.replace('\\', '/')

create_table = """
	CREATE TABLE new_comments(
		course_code VARCHAR(20),
		survey_id VARCHAR(15),
		offering_id INT,
		fiscal_year VARCHAR(9),
		quarter VARCHAR(5),
		learner_classif VARCHAR(80),
		learner_dept VARCHAR(150),
		offering_city_en VARCHAR(60),
		original_question VARCHAR(65),
		text_answer TEXT,
		offering_city_fr VARCHAR(60),
		short_question VARCHAR(60),
		overall_satisfaction TINYINT,
		learner_dept_code VARCHAR(75),
		nanos BOOL,
		stars TINYINT,
		magnitude FLOAT UNSIGNED,
		PRIMARY KEY(survey_id, original_question)
	);
"""

load_data = """
	LOAD DATA LOCAL INFILE '{0}/comments_processed_ML.csv'
	INTO TABLE new_comments
	FIELDS OPTIONALLY ENCLOSED BY '"'
	TERMINATED BY ','
	LINES TERMINATED BY '\r\n'
	IGNORE 1 LINES;
""".format(PROCESSED_DIR)

create_index = """
	CREATE INDEX idx_sq_cc_fy_ldc_strs ON new_comments(short_question, course_code, fiscal_year, learner_dept_code, stars);
"""

# Rename tables in a single atomic transaction
# Ensures clean switchover + no downtime
rename_tables = """
	RENAME TABLE comments TO old_comments, new_comments TO comments;
"""

drop_table = """
	DROP TABLE old_comments;
"""

try:
	run_mysql(cnx, create_table)
	logger.debug('2/7: New table created.')
	run_mysql(cnx, load_data)
	logger.debug('3/7: Data loaded.')
	run_mysql(cnx, create_index)
	logger.debug('4/7: Index created.')
	run_mysql(cnx, rename_tables)
	logger.debug('5/7: Tables renamed.')
	run_mysql(cnx, drop_table)
	logger.debug('6/7: Old table dropped.')
except Exception:
	logger.critical('Failure!', exc_info=True)
	cnx.close()
	exit()
finally:
	cnx.close()
	logger.debug('7/7: Connection closed.')
