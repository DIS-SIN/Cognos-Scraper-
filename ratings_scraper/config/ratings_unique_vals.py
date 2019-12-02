"""Target unique values for select fields in table 'ratings'. Stored
in Python sets for O(1) lookup times.
"""

ORIGINAL_QUESTION = {
	'Overall Satisfaction', # Only question persisted from old surveys
	'1. Satisfaction Overall',
	'3. Satisfaction - Level of detail of the content',
	'4. Satisfaction - Quality of the content',
	'5. Satisfaction - Language quality of the materials (English or French)',
	'6. Satisfaction - Quality of the graphics',
	'7. Satisfaction  Ease of navigation',
	'10. Before this learning activity',
	'11. After this learning activity',
	'12. Expectations Met',
	'13. Recommend learning Activity',
	'14. GCCampus Usage',
	'15. Videos',
	'16. Blogs',
	'17. Forums',
	'18. Job aids',
	'20. This learning activity is a valuable use of my time',
	'21. This learning activity is relevant to my job',
	'22. This learning activity is contributing to my performance on the job',
	'23. I can apply what I have learned on the job'
}

TEXT_ANSWER_EN = {
	'1',
	'2',
	'3',
	'4',
	'5',
	'6',
	'7',
	'8',
	'9',
	'10',
	'Agree', # From old surveys' field 'Overall Satisfaction'
	'Did not use tool',
	'Disagree', # From old surveys' field 'Overall Satisfaction'
	'Exceeded',
	'Likely',
	'Met',
	'Neither agree nor disagree', # From old surveys' field 'Overall Satisfaction'
	'No',
	'Not met',
	'Not valuable',
	'Somewhat likely',
	'Somewhat not valuable',
	'Somewhat unlikely',
	'Somewhat valuable',
	'Strongly agree', # From old surveys' field 'Overall Satisfaction'
	'Strongly disagree', # From old surveys' field 'Overall Satisfaction'
	'Unlikely',
	'Valuable',
	'Yes'
}
