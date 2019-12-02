"""Pre-process the extract from Cognos for integration into cloud DB."""
import logging
import os
import re
import pandas as pd
from config import shared_directories
from ratings_scraper.config import directories

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
# Files exported by Cognos have .csv extension but are tab-separated and
# encoded with UTF-16 Little Endian
# 'object' datatype in Pandas is synonymous with 'str'
os.chdir(shared_directories.DOWNLOADS_DIR)
df = pd.read_csv('Ratings.csv', sep='\t', index_col=False, encoding='utf_16_le',
				 keep_default_na=False)
if not df.shape[0] > 0:
	logger.critical('Failure: Ratings.csv is empty.')
	exit()

logger.debug('1/4: Data imported.')

# Ensure column 'course_code' is uppercase
df['course_code'] = df['course_code'].astype(str).str.upper()

# Remove whitespace from column 'fiscal_year' and switch format from '2019-2020' to
# '2019-20'
df['fiscal_year'] = df['fiscal_year'].astype(str).str.strip()
year_pattern = re.compile(pattern=r'([-]{1}\d{2})(?=\d{2})')
df['fiscal_year'] = df['fiscal_year'].astype(str).str.replace(year_pattern, '-', regex=True)

# Limit entries in column 'learner_classif' to 80 characters
# A very small number of learners put a lengthy description instead of their classification
df['learner_classif'] = df['learner_classif'].astype(str).str.slice(0, 80)

logger.debug('2/4: Data cleaned.')

# Import mapping for new column 'text_answer_fr'
os.chdir(directories.MAPPINGS_DIR)
text_answer_map = pd.read_csv('text_answer_map.csv', sep=',',
							  index_col=0, squeeze=True, encoding='utf-8')
if not text_answer_map.shape[0] > 0:
	logger.critical('Failure: text_answer_map.csv is empty.')
	exit()

# Import mapping for column 'learner_dept_code'
os.chdir(shared_directories.DEPARTMENT_CLEANING_DIR)
department_map = pd.read_csv('department_map.csv', sep=',', index_col=0,
							  squeeze=True, encoding='utf-8')
if not department_map.shape[0] > 0:
	logger.critical('Failure: department_map.csv is empty.')
	exit()

# Create new column 'text_answer_fr'
# Column 'text_answer_en' is entirely pre-defined answers like 'Yes' and 'No'
df['text_answer_fr'] = df['text_answer_en'].map(text_answer_map)

# Check if column 'text_answer_fr' properly mapped
# Unknown values would be assgined value 'np.nan', which has dtype 'float'
# Therefore, check all values have dtype 'str'
if not all([isinstance(val, str) for val in df['text_answer_fr'].unique()]):
	logger.critical('Failure: Unknown values in field text_answer_en')
	exit()

# Create new column 'learner_dept_code'
df['learner_dept_code'] = df['learner_dept'].map(department_map)

# Check if column 'learner_dept_code' properly mapped
# Unknown values would be assgined value 'np.nan', which has dtype 'float'
# Therefore, check all values have dtype 'str'
for tup in df.loc[:, ['learner_dept', 'learner_dept_code']].drop_duplicates().itertuples():
	if isinstance(tup[2], float):
		logger.info('New department entered in ratings: "{0}"'.format(tup[1]))

logger.debug('3/4: New columns created.')

# Export results as CSV
os.chdir(directories.PROCESSED_DIR)
df.to_csv('ratings_processed.csv', sep=',', encoding='utf-8', index=False,
		  quotechar='"', line_terminator='\r\n')

logger.debug('4/4: Data exported.')
