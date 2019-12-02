"""Check for unknown values in key fields. Unexpected changes can occasionally
appear due to poor communication with DBAs.
"""
import logging
import os
import pandas as pd
from config import shared_directories
from config import shared_unique_vals
from utils.utils import _check_column
from comments_scraper.config import comments_unique_vals

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
os.chdir(shared_directories.DOWNLOADS_DIR)
comments = pd.read_csv('Comments.csv', sep='\t', index_col=False, encoding='utf_16_le',
					   dtype={'survey_id': 'object'}, keep_default_na=False)
if not comments.shape[0] > 0:
	logger.critical('Failure: Comments.csv is empty.')
	exit()
logger.debug('1/4: Data imported.')

# Check column 'quarter'
_check_column(logger, comments['quarter'].unique(), shared_unique_vals.QUARTER)
logger.debug('2/4: Column \'quarter\' verified.')

# Check column 'original_question'
_check_column(logger, comments['original_question'].unique(), comments_unique_vals.ORIGINAL_QUESTION)
logger.debug('3/4: Column \'original_question\' verified.')

logger.debug('4/4: Check complete: No unknown values.')
