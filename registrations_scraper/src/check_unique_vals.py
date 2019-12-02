"""Check for unknown values in key fields. Unexpected changes can occasionally
appear due to poor communication with DBAs.
"""
import logging
import os
import pandas as pd
from config import shared_directories
from config import shared_unique_vals
from utils.utils import _check_column
from registrations_scraper.config import registrations_unique_vals

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
os.chdir(shared_directories.DOWNLOADS_DIR)
regs = pd.read_csv('Registrations.csv', sep='\t', index_col=False, encoding='utf_16_le',
				   keep_default_na=False)
if not regs.shape[0] > 0:
	logger.critical('Failure: Registrations.csv is empty.')
	exit()
logger.debug('1/16: Data imported.')

# Check column 'business_type'
_check_column(logger, regs['business_type'].unique(), registrations_unique_vals.BUSINESS_TYPE)
logger.debug('2/16: Column \'business_type\' verified.')

# Check column 'month_en'
_check_column(logger, regs['month_en'].unique(), shared_unique_vals.MONTH_EN)
logger.debug('3/16: Column \'month_en\' verified.')

# Check column 'month_fr'
_check_column(logger, regs['month_fr'].unique(), shared_unique_vals.MONTH_FR)
logger.debug('4/16: Column \'month_fr\' verified.')

# Check column 'offering_status'
_check_column(logger, regs['offering_status'].unique(), shared_unique_vals.OFFERING_STATUS)
logger.debug('5/16: Column \'offering_status\' verified.')

# Check column 'offering_language'
_check_column(logger, regs['offering_language'].unique(), shared_unique_vals.OFFERING_LANGUAGE)
logger.debug('6/16: Column \'offering_language\' verified.')

# Check column 'offering_region_en'
_check_column(logger, regs['offering_region_en'].unique(), shared_unique_vals.REGION_EN)
logger.debug('7/16: Column \'offering_region_en\' verified.')

# Check column 'offering_region_fr'
_check_column(logger, regs['offering_region_fr'].unique(), shared_unique_vals.REGION_FR)
logger.debug('8/16: Column \'offering_region_fr\' verified.')

# Check column 'offering_province_en'
_check_column(logger, regs['offering_province_en'].unique(), shared_unique_vals.PROVINCE_EN)
logger.debug('9/16: Column \'offering_province_en\' verified.')

# Check column 'offering_province_fr'
_check_column(logger, regs['offering_province_fr'].unique(), shared_unique_vals.PROVINCE_FR)
logger.debug('10/16: Column \'offering_province_fr\' verified.')

# Check column 'learner_province_en'
_check_column(logger, regs['learner_province_en'].unique(), shared_unique_vals.PROVINCE_EN)
logger.debug('11/16: Column \'learner_province_en\' verified.')

# Check column 'learner_province_fr'
_check_column(logger, regs['learner_province_fr'].unique(), shared_unique_vals.PROVINCE_FR)
logger.debug('12/16: Column \'learner_province_fr\' verified.')

# Check column 'reg_status'
_check_column(logger, regs['reg_status'].unique(), registrations_unique_vals.REG_STATUS)
logger.debug('13/16: Column \'reg_status\' verified.')

# Check column 'no_show'
_check_column(logger, regs['no_show'].unique(), registrations_unique_vals.NO_SHOW)
logger.debug('14/16: Column \'no_show\' verified.')

# Check column 'learner_language'
_check_column(logger, regs['learner_language'].unique(), registrations_unique_vals.LEARNER_LANGUAGE)
logger.debug('15/16: Column \'learner_language\' verified.')

logger.debug('16/16: Check complete: No unknown values.')
