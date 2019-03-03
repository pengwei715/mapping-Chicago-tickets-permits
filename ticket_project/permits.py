'''
Interface of Permits:
https://data.cityofchicago.org/Transportation/Transportation-Department-Permits/pubx-yq2d/data
Identify approved permits, 
permits that actually resulted in street closures, 
trim data to eliminate unnecessary columns
Find proper way interface with API and load in ata
'''

#!/usr/bin/env python

#requre the sodapy library
import os
import sys
import pandas
import requests
import json
import csv
from sodapy import Socrata


DATA_DIR = "./data/"

def get_sample(num, filename):
	'''
	Get some sample permit data
	Input:
	    num: number of rows 
	    filename: the name of the csv file 
	Return:
	    No return, the csv file contains the sample data.
	'''

	# Unauthenticated client only works with public data sets. Note 'None'
	# in place of application token, and no username or password:
	client = Socrata("data.cityofchicago.org", None)

	# First 2000 results, returned as JSON from API / converted to Python list of
	# dictionaries by sodapy.
	results = client.get("erhc-fkv9", limit=num)

