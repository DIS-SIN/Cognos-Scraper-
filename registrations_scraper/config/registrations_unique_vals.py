"""Target unique values for select fields in table 'lsr_this_year'. Stored
in Python sets for O(1) lookup times.
"""

# Tables 'lsr_this_year' and 'lsr_last_year' exclude events, unlike table 'offerings'
BUSINESS_TYPE = {'Instructor-Led', 'Online'}

LEARNER_LANGUAGE = {'English', 'French'}

NO_SHOW = {0, 1}

REG_STATUS = {
	'Cancelled',
	'Cancelled - Class Cancelled',
	'Confirmed',
	'Offered',
	'Waitlisted'
}
