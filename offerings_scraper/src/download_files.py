"""Download list of offerings as CSV from Cognos."""
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

# Delete previous raw data download
os.chdir(shared_directories.DOWNLOADS_DIR)
delete_files_of_this_ilk('Offerings')
logger.debug('1/7: Previous files deleted.')

# Open virtual viewport
display = Display(visible=0, size=(1920, 1080))
display.start()
logger.debug('2/7: Virtual viewport opened.')

# Open controlled browser
browser = webdriver.Chrome()
logger.debug('3/7: Browser opened.')

# Navigate to Cognos and login
browser.get(os.environ.get('LOGIN_URL'))
browser.find_element_by_id('CAMUsername').send_keys(USERNAME)
browser.find_element_by_id('CAMPassword').send_keys(PASSWORD)
browser.find_element_by_id('cmdOK').click()
logger.debug('4/7: Logged in to Cognos.')

# Download offerings
browser.get(os.environ.get('OFFERINGS_URL'))
os.chdir(shared_directories.DOWNLOADS_DIR)
if not check_file_exists('Offerings.csv'):
	logger.critical('Failure: Offerings download unsuccessful.')
	exit()
logger.debug('5/7: Offerings downloaded.')

# Logout
browser.find_elements_by_css_selector('#_NS_logOnOff td')[0].click()
logger.debug('6/7: Logged out of Cognos.')

# End module
browser.quit()
display.stop()
logger.debug('7/7: Module ended.')
