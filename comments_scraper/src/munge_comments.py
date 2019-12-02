"""Pre-process the extract from Cognos for integration into cloud DB."""
import logging
import os
import re
import pandas as pd
from config import shared_directories
from config.shared_mappings import city_map
from comments_scraper.config import directories
from comments_scraper.config.comments_unique_vals import check_nanos

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
# Files exported by Cognos have .csv extension but are tab-separated and
# encoded with UTF-16 Little Endian
# 'object' datatype in Pandas is synonymous with 'str'
os.chdir(shared_directories.DOWNLOADS_DIR)
comments = pd.read_csv('Comments.csv', sep='\t', index_col=False, encoding='utf_16_le',
					   dtype={'survey_id': 'object'}, keep_default_na=False)
if not comments.shape[0] > 0:
	logger.critical('Failure: Comments.csv is empty.')
	exit()

logger.debug('1/4: Data imported.')

# Ensure column 'course_code' is uppercase
comments['course_code'] = comments['course_code'].astype(str).str.upper()

# Remove whitespace from column 'fiscal_year' and switch format from '2019-2020' to
# '2019-20'
comments['fiscal_year'] = comments['fiscal_year'].astype(str).str.strip()
year_pattern = re.compile(pattern=r'([-]{1}\d{2})(?=\d{2})')
comments['fiscal_year'] = comments['fiscal_year'].astype(str).str.replace(year_pattern, '-', regex=True)

# Limit entries in column 'learner_classif' to 80 characters
# A very small number of learners put a lengthy description instead of their classification
comments['learner_classif'] = comments['learner_classif'].astype(str).str.slice(0, 80)

logger.debug('2/4: Data cleaned.')

# Create new column 'offering_city_fr' as certain cities require translation e.g. 'NCR'
comments['offering_city_fr'] = comments['offering_city_en'].map(city_map)

### IMPORT MAPPINGS ###

# Import mapping to overwrite column 'short_question'
os.chdir(directories.MAPPINGS_DIR)
short_question_map = pd.read_csv('short_question_map.csv', sep=',',
								 index_col=0, squeeze=True, encoding='utf-8')
if not short_question_map.shape[0] > 0:
	logger.critical('Failure: short_question_map.csv is empty.')
	exit()

# Import mapping for column 'overall_satisfaction'
os.chdir(shared_directories.DOWNLOADS_DIR)
overall_sat_map = pd.read_csv('Overall Satisfaction.csv', sep='\t', index_col=0,
							  squeeze=True, encoding='utf_16_le')
if not overall_sat_map.shape[0] > 0:
	logger.critical('Failure: Overall Satisfaction.csv is empty.')
	exit()

# Import mapping for column 'learner_dept_code'
os.chdir(shared_directories.DEPARTMENT_CLEANING_DIR)
department_map = pd.read_csv('department_map.csv', sep=',', index_col=0,
							  squeeze=True, encoding='utf-8')
if not department_map.shape[0] > 0:
	logger.critical('Failure: department_map.csv is empty.')
	exit()

# Create new column 'short_question'
# Stores re-mapped questions e.g. 'Issue Description' and its variants all mapped to
# 'Comment - Technical'
comments['short_question'] = comments['original_question'].map(short_question_map)

# Check if column 'short_question' properly mapped
# Unknown values would be assgined value 'np.nan', which has dtype 'float'
# Therefore, check all values have dtype 'str'
if not all([isinstance(short_question, str) for short_question in comments['short_question'].unique()]):
	logger.critical('Failure: Unknown values in field original_question')
	exit()

# Create new column 'overall_satisfaction'
# Rarely, a learner will have left a comment without indicating overall satisfaction
# Assign these comments value '\N' (null integer in MySQL)
comments['overall_satisfaction'] = comments['survey_id'].map(overall_sat_map).fillna('\\N')

# Create new column 'learner_dept_code'
comments['learner_dept_code'] = comments['learner_dept'].map(department_map)

# Check if column 'learner_dept_code' properly mapped
# Unknown values would be assgined value 'np.nan', which has dtype 'float'
# Therefore, check all values have dtype 'str'
for tup in comments.loc[:, ['learner_dept', 'learner_dept_code']].drop_duplicates().itertuples():
	if isinstance(tup[2], float):
		logger.info('New department entered in comments: "{0}"'.format(tup[1]))

# Create boolean column 'nanos' indicating if response from new survey
comments['nanos'] = comments['original_question'].map(check_nanos)

logger.debug('3/4: New columns created.')

# Export results as CSV
os.chdir(directories.PROCESSED_DIR)
comments.to_csv('comments_processed.csv', sep=',', encoding='utf-8', index=False,
				quotechar='"', line_terminator='\r\n')

logger.debug('4/4: Data exported.')
