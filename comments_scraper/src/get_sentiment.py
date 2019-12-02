"""Get sentiment scores for comments from the Google NLP API."""
import logging
from decimal import Decimal
import os
import pickle
import time
from google.cloud import language
from google.api_core.exceptions import InvalidArgument
import pandas as pd
from utils.utils import _check_col_in_valid_range
from comments_scraper.config import directories

# Instantiate logger
logger = logging.getLogger(__name__)

# Load dataset
os.chdir(directories.PROCESSED_DIR)
df = pd.read_csv('comments_processed.csv', sep=',', index_col=False,
				 encoding='utf-8', dtype={'survey_id': 'object'}, keep_default_na=False)
if not df.shape[0] > 0:
	logger.critical('Failure: comments_processed.csv is empty.')
	exit()

logger.debug('1/5: Data imported.')

# Load pickle for memoization
os.chdir(directories.PICKLE_DIR)
with open('sentiment_dict.pickle', 'rb') as f:
	sentiment_dict = pickle.load(f)

logger.debug('2/5: Pickle imported.')

# Instantiate client
client = language.LanguageServiceClient()

# Total number of comments processed
ctr = 0
# New comments passed to API
api_ctr = 0


def get_sentiment_score(survey_id, original_question, text_answer, overall_satisfaction):
	"""Pass text to API, return its sentiment score, and memoize results."""
	global ctr
	global api_ctr
	ctr += 1
	# Log ctr every 10k comments
	if ctr % 10_000 == 0:
		logger.debug('Finished {0} comments.'.format(ctr))
	
	# Use composite key of survey_id.original_question
	pkey = '{0}.{1}'.format(survey_id, original_question)
	
	# If already processed, returned memoized result to save compute
	if pkey in sentiment_dict:
		return sentiment_dict[pkey]
	
	# Otherwise, pass to API
	api_ctr += 1
	# API has limit of 600 queries / min
	if api_ctr % 590 == 0:
		time.sleep(60)
	
	try:
		document = language.types.Document(content=text_answer,
										   type=language.enums.Document.Type.PLAIN_TEXT)
		sentiment = client.analyze_sentiment(document=document).document_sentiment
		# Adjust sentiment scores from real numbers in [-1, 1] to integers in [1, 5]
		# Cast to Decimal then back to int to prevent floating point rounding errors
		sent = int(round(Decimal(str((sentiment.score * 2) + 3))))
		mag = sentiment.magnitude
	# Comments occasionally so badly written the API can't identify the language
	except InvalidArgument as e:
		logger.info('API failed to process: {0}'.format(text_answer))
		# Default to overall_satisfaction
		if overall_satisfaction == '\\N':
			sent = '\\N'
			mag = '\\N'
		else:
			sent = int(float(overall_satisfaction) / 2)
			mag = '\\N'
	
	# Memoize and return result
	result = (sent, mag)
	sentiment_dict[pkey] = result
	return result


api_results = df.apply(lambda x: get_sentiment_score(x['survey_id'], x['original_question'], x['text_answer'], x['overall_satisfaction']),
					   axis=1,					# Apply to each row
					   raw=False,				# Pass each cell individually as not using NumPy
					   result_type='expand')	# Return DataFrame rather than Series of tuples
df['stars'] = api_results.loc[:, 0]
df['magnitude'] = api_results.loc[:, 1]

# Ensure new columns in valid range
if not _check_col_in_valid_range(df['stars'].unique(), 1, 5):
	logger.critical('Failure: Invalid stars.')
	exit()
if not _check_col_in_valid_range(df['magnitude'].unique(), 0, float('inf')):
	logger.critical('Failure: Invalid magnitude.')
	exit()

logger.debug('3/5: New column created; {0} new comments ML\'d.'.format(api_ctr))

# Export sentiment_dict to pickle for future re-use
os.chdir(directories.PICKLE_DIR)
with open('sentiment_dict.pickle', 'wb') as f:
	pickle.dump(sentiment_dict, f, protocol=pickle.HIGHEST_PROTOCOL)

logger.debug('4/5: Pickle exported.')

# Export results as CSV
os.chdir(directories.PROCESSED_DIR)
df.to_csv('comments_processed_ML.csv', sep=',', encoding='utf-8', index=False,
		  quotechar='"', line_terminator='\r\n')

logger.debug('5/5: Data exported.')
