"""Pre-process the extract from Cognos for integration into cloud DB."""
import logging
import os
import re
import pandas as pd
from config import shared_directories
from config.shared_mappings import city_map
from offerings_scraper.config import directories

# Instantiate logger
logger = logging.getLogger(__name__)

### IMPORT RAW DATA ###
# Files exported by Cognos have .csv extension but are tab-separated and
# encoded with UTF-16 Little Endian
# 'object' datatype in Pandas is synonymous with 'str'
os.chdir(shared_directories.DOWNLOADS_DIR)
df = pd.read_csv('Offerings.csv', sep='\t', index_col=False, encoding='utf_16_le',
				 keep_default_na=False)
if not df.shape[0] > 0:
	logger.critical('Failure: Offerings.csv is empty.')
	exit()

logger.debug('1/4: Data imported.')

# Ensure column 'course_code' is uppercase
df['course_code'] = df['course_code'].astype(str).str.upper()

# Remove whitespace from column 'fiscal_year' and switch format from e.g. '2019-2020' to
# '2019-20'
df['fiscal_year'] = df['fiscal_year'].astype(str).str.strip()
year_pattern = re.compile(pattern=r'([-]{1}\d{2})(?=\d{2})')
df['fiscal_year'] = df['fiscal_year'].astype(str).str.replace(year_pattern, '-', regex=True)

# Remove whitespace from columns known to contain junk spacing
df['instructor_names'] = df['instructor_names'].astype(str).str.strip()
df['client'] = df['client'].astype(str).str.strip()
df['event_description'] = df['event_description'].astype(str).str.strip()

# Merge obscure values with standard values for 'offering_language'
df['offering_language'] = df['offering_language'].astype(str).str.replace('Simultaneous Translation ', 'Bilingual').replace('ESL', 'English').replace('FSL', 'French')

logger.debug('2/4: Data cleaned.')

# Create new column 'offering_city_fr' as certain cities require translation e.g. 'NCR'
df['offering_city_fr'] = df['offering_city_en'].map(city_map)

logger.debug('3/4: New columns created.')

# Export results as CSV
os.chdir(directories.PROCESSED_DIR)
df.to_csv('offerings_processed.csv', sep=',', encoding='utf-8', index=False,
		  quotechar='"', line_terminator='\r\n')

logger.debug('4/4: Data exported.')
