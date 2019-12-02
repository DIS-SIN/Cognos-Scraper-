"""Download raw data (comments and their associated overall satisfaction scores)
as CSVs from Cognos.
"""
import logging
import os
from pyvirtualdisplay import Display
from selenium import webdriver
from config import shared_directories
from utils.file_system import check_file_exists, delete_files_of_this_ilk

# Instantiate logger
logger = logging.getLogger(__name__)

# Cognos login
USERNAME = os.environ.get('COGNOS_USERNAME', None)
PASSWORD = os.environ.get('COGNOS_PASSWORD', None)
if USERNAME is None:
	logger.critical('Failure: Missing environ var USERNAME.')
	exit()
if PASSWORD is None:
	logger.critical('Failure: Missing environ var PASSWORD.')
	exit()

# Delete previous raw data downloads
os.chdir(shared_directories.DOWNLOADS_DIR)
delete_files_of_this_ilk('Comments')
delete_files_of_this_ilk('Overall Satisfaction')
logger.debug('1/8: Previous files deleted.')

# Open virtual viewport
display = Display(visible=0, size=(1920, 1080))
display.start()
logger.debug('2/8: Virtual viewport opened.')

# Open controlled browser
browser = webdriver.Chrome()
logger.debug('3/8: Browser opened.')

# Navigate to Cognos and login
browser.get(os.environ.get('LOGIN_URL'))
browser.find_element_by_id('CAMUsername').send_keys(USERNAME)
browser.find_element_by_id('CAMPassword').send_keys(PASSWORD)
browser.find_element_by_id('cmdOK').click()
logger.debug('4/8: Logged in to Cognos.')

# Download comments
browser.get(os.environ.get('COMMENTS_URL'))
os.chdir(shared_directories.DOWNLOADS_DIR)
if not check_file_exists('Comments.csv'):
	logger.critical('Failure: Comments download unsuccessful')
	exit()
logger.debug('5/8: Comments downloaded.')

# Download overall satisfaction
browser.get(os.environ.get('OVERALL_SATISFACTION_URL'))
os.chdir(shared_directories.DOWNLOADS_DIR)
if not check_file_exists('Overall Satisfaction.csv'):
	logger.critical('Failure: Overall satisfaction download unsuccessful')
	exit()
logger.debug('6/8: Overall satisfaction downloaded.')

# Logout
browser.find_elements_by_css_selector('#_NS_logOnOff td')[0].click()
logger.debug('7/8: Logged out of Cognos.')

# End module
browser.quit()
display.stop()
logger.debug('8/8: Module ended.')
