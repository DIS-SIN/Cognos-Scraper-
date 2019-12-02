"""Check for unknown values in key fields. Unexpected changes can occasionally
appear due to poor communication with DBAs.
"""
import logging
import os
import pandas as pd
from config import shared_directories
from config import shared_unique_vals
from utils.utils import _check_column
from offerings_scraper.config import offerings_unique_vals

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
os.chdir(shared_directories.DOWNLOADS_DIR)
df = pd.read_csv('Offerings.csv', sep='\t', index_col=False, encoding='utf_16_le',
				 keep_default_na=False)
if not df.shape[0] > 0:
	logger.critical('Failure: Offerings.csv is empty.')
	exit()
logger.debug('1/10: Data imported.')

# Check column 'business_type'
_check_column(logger, df['business_type'].unique(), offerings_unique_vals.BUSINESS_TYPE)
logger.debug('2/10: Column \'business_type\' verified.')

# Check column 'quarter'
_check_column(logger, df['quarter'].unique(), shared_unique_vals.QUARTER)
logger.debug('3/10: Column \'quarter\' verified.')

# Check column 'offering_status'
_check_column(logger, df['offering_status'].unique(), shared_unique_vals.OFFERING_STATUS)
logger.debug('4/10: Column \'offering_status\' verified.')

# Check column 'offering_language'
_check_column(logger, df['offering_language'].unique(), shared_unique_vals.OFFERING_LANGUAGE)
logger.debug('5/10: Column \'offering_language\' verified.')

# Check column 'offering_region_en'
_check_column(logger, df['offering_region_en'].unique(), shared_unique_vals.REGION_EN)
logger.debug('6/10: Column \'offering_region_en\' verified.')

# Check column 'offering_region_fr'
_check_column(logger, df['offering_region_fr'].unique(), shared_unique_vals.REGION_FR)
logger.debug('7/10: Column \'offering_region_fr\' verified.')

# Check column 'offering_province_en'
_check_column(logger, df['offering_province_en'].unique(), shared_unique_vals.PROVINCE_EN)
logger.debug('8/10: Column \'offering_province_en\' verified.')

# Check column 'offering_province_fr'
_check_column(logger, df['offering_province_fr'].unique(), shared_unique_vals.PROVINCE_FR)
logger.debug('9/10: Column \'offering_province_fr\' verified.')

logger.debug('10/10: Check complete: No unknown values.')
