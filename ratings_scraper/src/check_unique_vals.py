"""Check for unknown values in key fields. Unexpected changes can occasionally
appear due to poor communication with DBAs.
"""
import logging
import os
import pandas as pd
from config import shared_directories
from config import shared_unique_vals
from utils.utils import _check_column
from ratings_scraper.config import ratings_unique_vals

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
os.chdir(shared_directories.DOWNLOADS_DIR)
df = pd.read_csv('Ratings.csv', sep='\t', index_col=False, encoding='utf_16_le',
				 keep_default_na=False)
if not df.shape[0] > 0:
	logger.critical('Failure: Ratings.csv is empty.')
	exit()
logger.debug('1/7: Data imported.')

# Check column 'month_en'
_check_column(logger, df['month_en'].unique(), shared_unique_vals.MONTH_EN)
logger.debug('2/7: Column \'month_en\' verified.')

# Check column 'month_fr'
_check_column(logger, df['month_fr'].unique(), shared_unique_vals.MONTH_FR)
logger.debug('3/7: Column \'month_fr\' verified.')

# Check column 'original_question'
_check_column(logger, df['original_question'].unique(), ratings_unique_vals.ORIGINAL_QUESTION)
logger.debug('4/7: Column \'original_question\' verified.')

# Check column 'numerical_answer'
# Recall that func 'range' has format [0, 11) i.e. [0, 10]
_check_column(logger, df['numerical_answer'].unique(), range(0, 11))
logger.debug('5/7: Column \'numerical_answer\' verified.')

# Check column 'text_answer_en'
_check_column(logger, df['text_answer_en'].unique(), ratings_unique_vals.TEXT_ANSWER_EN)
logger.debug('6/7: Column \'text_answer_en\' verified.')

logger.debug('7/7: Check complete: No unknown values.')
