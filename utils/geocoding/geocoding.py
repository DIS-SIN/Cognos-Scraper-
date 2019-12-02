"""Geocode cities via the Google Geocoding API."""
import json
import logging
import os
import requests
import urllib

# Instantiate logger
logger = logging.getLogger(__name__)

# Check if credentials stored in environ vars
API_KEY = os.environ.get('GOOGLE_GEOCODING_KEY', None)
if API_KEY is None:
	logger.critical('Failure: Missing Google NLP API credentials.')
	exit()


def _get_memo_name(city, prov):
	"""Create composite key of format 'City, Province' for memoization."""
	return '{0}, {1}'.format(city, prov)


def _get_lookup_name(city, prov):
	"""Remove junk strings that interfere with Geocoding API"""
	return '{0}, {1}'.format(city, prov).replace(', Outside Canada', '').replace(', Unknown', '')


def get_lat_lng(city, prov, geo_dict, fail_ctr):
	"""
	Get a city's latitude and longitude from the Google Maps Geocoding API and memoize.
	"""
	# Format city name for memoization
	memo_city = _get_memo_name(city, prov)
	
	# Format city name for API
	lookup_city = _get_lookup_name(city, prov)
	
	# Memoization
	if memo_city in geo_dict:
		return geo_dict[memo_city]
	
	# Build query; use urllib to properly encode non-ASCII chars
	url = "https://maps.googleapis.com/maps/api/geocode/json?"
	url_vars = {'address': lookup_city, 'key': API_KEY, 'region': 'ca'}
	my_query = url + urllib.parse.urlencode(url_vars)
	geo_request = requests.get(my_query)
	
	# Check if request was successful
	if geo_request.status_code == 200:
		geo_response = json.loads(geo_request.text)
	else:
		logger.warning('Request error with city {0}: {1}'.format(memo_city, geo_request.status_code))
		fail_ctr += 1
		# Allow up to 3 network or API errors
		if fail_ctr >= 3:
			exit()
		return {'lat': '\\N', 'lng': '\\N'}
	
	# Check if API response status is 'OK'
	api_status = geo_response['status']
	if api_status != 'OK':
		logger.warning('API error with city {0}: {1}'.format(memo_city, api_status))
		# Allow up to 3 network or API errors
		fail_ctr += 1
		if fail_ctr >= 3:
			exit()
		# Assume the city was junk entered in GCcampus and assign it null coördinates
		# Log the assignment to Slack so can be confirmed as junk
		results = {'lat': '\\N', 'lng': '\\N'}
		geo_dict[memo_city] = results
		logger.info('{0} is assumed to be junk and has been assigned null coördinates.'.format(memo_city))
		return results
	
	# Parse results and memoize
	lat = geo_response['results'][0]['geometry']['location']['lat']
	lng = geo_response['results'][0]['geometry']['location']['lng']
	results = {'lat': lat, 'lng': lng}
	geo_dict[memo_city] = results
	logger.info('New city geocoded: {0} at {1}, {2}.'.format(lookup_city, lat, lng))
	
	return results
