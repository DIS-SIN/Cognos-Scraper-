"""General shared functions and classes."""


def _check_column(logger, col_vals, target_vals):
	"""Compare a column's unique values to target set."""
	for elem in col_vals:
		if elem not in target_vals:
			logger.critical('Failure: Unknown value \'{0}\' in latest Cognos extract.'.format(elem))
			exit()


def _check_col_in_valid_range(my_list, min, max):
	"""Ensure values are within specific range or a MySQL null (i.e. '\\N')."""
	for val in my_list:
		try:
			val_float = float(val)
		except ValueError:
			if val != '\\N':
				return False
		else:
			if not (min <= val_float <= max):
				return False
	return True
