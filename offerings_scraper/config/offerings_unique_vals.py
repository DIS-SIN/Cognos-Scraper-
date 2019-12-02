"""Target unique values for select fields in table 'offerings'. Stored
in Python sets for O(1) lookup times.
"""

# Table 'offerings' includes events, unlike tables 'lsr_this_year' and 'lsr_last_year'
BUSINESS_TYPE = {'Events', 'Instructor-Led', 'Online'}
