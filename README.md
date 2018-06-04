# EDGAR-request-pipeline

For this challenge, I take existing publicly available EDGAR weblogs and assume that each line represents a single web request for an EDGAR document that would be streamed into my program in real time.

Using the data, identify when a user visits, calculate the duration of and number of documents requested during that visit, and then write the output to a file.

In order to be memory efficient, I store all active sessions in two-layer nested dictionary structure. The inner dictionary stores session details while the outer dictionary is uniquely 
hashed by user ip address. This is to achieve fast memory access.

Upon recording ended user sessions (indicated by an input max inactivity period) into output file,
session items are sorted first by the time of first user request and then by line number in the
input file. Information is recorded on file in the same order.

# Requirements
- Python 3

# Python Libraries
- argparse
- datetime
