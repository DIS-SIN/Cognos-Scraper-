import json
import logging
import os
import requests
import time

assert os.environ.get('SLACK_BOT_URL', None) is not None, 'Missing environ var SLACK_BOT_URL.'

class SlackHandler(logging.Handler):
	"""Custom handler to store log record in JSON and send in POST request."""
	def emit(self, record):
		"""Set headers and URL."""
		log_entry = self.format(record)
		url = os.environ.get('SLACK_BOT_URL')
		return requests.post(url, log_entry, headers={'Content-type': 'application/json'}).content


class SlackFormatter(logging.Formatter):
	"""Custom formatter to structure JSON object as required by Slack."""
	def __init__(self):
		super(SlackFormatter, self).__init__()
	
	def format(self, record):
		"""Unpack record object and assemble into string."""
		asc_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(record.created)))
		level_name = record.levelname
		name = record.name
		message = record.msg
		exc_info = record.exc_info
		return_string = f'{asc_time} - {level_name} - {name} - {message} - { exc_info if exc_info else "" }'
		data = {'text': return_string}
		return json.dumps(data)


class ThirdPartyFilter(logging.Filter):
	"""Exclude unnecessary logs from 3rd party libraries."""
	def filter(self, record):
		"""Required func returning boolean indicating if record to be emitted by handler."""
		IGNORE_LIST = ['easyprocess', 'google', 'pyvirtualdisplay', 'selenium']
		check = [record.name.startswith(val) for val in IGNORE_LIST]
		if any(check):
			return False
		return True

# Add custom handler to logging library
logging.handlers.SlackHandler = SlackHandler

logger_dict = {
	'version': 1,
	'filters': {
		'thirdPartyFilter': {
			'()': ThirdPartyFilter
		}
	},
	'formatters': {
		'formatter': {
			'format': '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
			'datefmt': '%Y-%m-%d %H:%M:%S'
		},
		'slackFormatter': {
			'()': SlackFormatter
		}
	},
	'handlers': {
		'fileHandler': {
			'class': 'logging.FileHandler',
			'level': 'DEBUG',
			'formatter': 'formatter',
			'encoding': 'utf-8',
			'filename': 'logs/scraper.log',
			'filters': ['thirdPartyFilter']
		},
		'stdOutHandler': {
			'class': 'logging.StreamHandler',
			'level': 'DEBUG',
			'formatter': 'formatter',
			'stream': 'ext://sys.stdout',
			'filters': ['thirdPartyFilter']
		},
		'slackHandler': {
			'class': 'logging.handlers.SlackHandler',
			'level': 'INFO',
			'formatter': 'slackFormatter',
			'filters': ['thirdPartyFilter']
		}
	},
	'loggers': {
		# Root logger identified with empty string
		'': {
			'handlers': ['fileHandler', 'stdOutHandler', 'slackHandler'],
			'level': 'NOTSET'
		}
	}
}
