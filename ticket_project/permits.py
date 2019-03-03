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

def get_sample(num, filename = "permits_sample.csv"):
    '''
    Get some sample permit data
    Input:
        num: number of rows 
        filename: the name of the csv file 
    Return:
        No return, the csv file contains the sample data.
    '''
    client = Socrata("data.cityofchicago.org", None)

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get("erhc-fkv9", limit=num)

    temp = set()
    for item in results:
        temp.update(set(item.keys()))
    csv_columns = temp

    csv_file = DATA_DIR + filename
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in results:
                writer.writerow(data)
    except IOError:
        print("I/O error")

