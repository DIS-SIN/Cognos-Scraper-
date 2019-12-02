"""Pre-process the extract from Cognos for integration into cloud DB."""
import logging
import os
import re
import pandas as pd
from config import shared_directories
from departments_scraper.config import directories

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
# Files exported by Cognos have .csv extension but are tab-separated and
# encoded with UTF-16 Little Endian
# 'object' datatype in Pandas is synonymous with 'str'
os.chdir(shared_directories.DOWNLOADS_DIR)
df = pd.read_csv('Departments.csv', sep='\t', index_col=False, encoding='utf_16_le',
				 keep_default_na=False)
if not df.shape[0] > 0:
	logger.critical('Failure: Departments.csv is empty.')
	exit()

logger.debug('1/3: Data imported.')

# Ensure column 'dept_code' is uppercase
df['dept_code'] = df['dept_code'].astype(str).str.upper()

# Remove junk prefixes
df['dept_name_en'] = df['dept_name_en'].astype(str).str.replace('_archive_', ' ').replace('_obsolete_', ' ').replace('_Obsolete_', ' ')
df['dept_name_fr'] = df['dept_name_fr'].astype(str).str.replace('_archive_', ' ').replace('_obsolete_', ' ').replace('_Obsolete_', ' ')

# Remove superfluous whitespace
df['dept_name_en'] = df['dept_name_en'].astype(str).str.strip()
df['dept_name_fr'] = df['dept_name_fr'].astype(str).str.strip()

logger.debug('2/3: Data cleaned.')

# Export results as CSV
os.chdir(directories.PROCESSED_DIR)
df.to_csv('departments_processed.csv', sep=',', encoding='utf-8', index=False,
		  quotechar='"', line_terminator='\r\n')

logger.debug('3/3: Data exported.')
