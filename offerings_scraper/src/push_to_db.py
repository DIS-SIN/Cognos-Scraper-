"""Push processed extract to cloud DB."""
import logging
from offerings_scraper.config.directories import PROCESSED_DIR
from utils.db import get_db, run_mysql

# Instantiate logger
logger = logging.getLogger(__name__)

# Store DB connection in global var to avoid reconnecting after each query
cnx = get_db()
logger.debug('1/7: Connected to DB.')

# MySQL requires paths with forward slashes
PROCESSED_DIR = PROCESSED_DIR.replace('\\', '/')

create_table = """
	CREATE TABLE new_offerings(
		offering_id INT PRIMARY KEY,
		course_title_en VARCHAR(200),
		course_title_fr VARCHAR(300),
		course_code VARCHAR(20),
		instructor_names VARCHAR(200),
		confirmed_count SMALLINT UNSIGNED,
		cancelled_count SMALLINT UNSIGNED,
		waitlisted_count SMALLINT UNSIGNED,
		no_show_count SMALLINT UNSIGNED,
		business_type VARCHAR(30),
		event_description TEXT,
		fiscal_year VARCHAR(7),
		quarter VARCHAR(2),
		start_date DATE,
		end_date DATE,
		client VARCHAR(50),
		offering_status VARCHAR(30),
		offering_language VARCHAR(50),
		offering_region_en VARCHAR(30),
		offering_region_fr VARCHAR(30),
		offering_province_en VARCHAR(30),
		offering_province_fr VARCHAR(30),
		offering_city_en VARCHAR(50),
		offering_city_fr VARCHAR(50),
		offering_lat DECIMAL(10, 8),
		offering_lng DECIMAL(11, 8)
	);
"""

load_data = """
	LOAD DATA LOCAL INFILE '{0}/offerings_processed_geo.csv'
	INTO TABLE new_offerings
	FIELDS OPTIONALLY ENCLOSED BY '"'
	TERMINATED BY ','
	LINES TERMINATED BY '\r\n'
	IGNORE 1 LINES
	(offering_id, course_title_en, course_title_fr, course_code, instructor_names,
	confirmed_count, cancelled_count, waitlisted_count, no_show_count, business_type, event_description,
	fiscal_year, quarter, @temp_start_date, @temp_end_date, client, offering_status, offering_language,
	offering_region_en,	offering_region_fr, offering_province_en, offering_province_fr,
	offering_city_en, offering_city_fr, offering_lat, offering_lng)
	SET start_date = STR_TO_DATE(@temp_start_date, '%Y-%m-%d %T'),
	end_date = STR_TO_DATE(@temp_end_date, '%Y-%m-%d %T');
""".format(PROCESSED_DIR)

indices = []

# Rename tables in a single atomic transaction
# Ensures clean switchover + no downtime
rename_tables = """
	RENAME TABLE offerings TO old_offerings, new_offerings TO offerings;
"""

drop_table = """
	DROP TABLE old_offerings;
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
