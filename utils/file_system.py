import glob
import os
import time


def check_file_exists(filename):
	"""Check every 5 seconds if file exists in current working directory."""
	ctr = 0
	while not os.path.isfile(filename):
		if ctr == 60:
			return False
		time.sleep(5)
		ctr += 1
	return True


def delete_files_of_this_ilk(ilk):
	"""Delete 'Foo.csv', 'Foo (1).csv', etc. from current working directory."""
	files = glob.glob('{0}*.csv'.format(ilk))
	for file in files:
		try:
			os.remove(file)
		except FileNotFoundError:
			pass
