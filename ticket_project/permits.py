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
import pandas as pd
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
    if num == -1:
        num = None
    results = client.get("erhc-fkv9", limit=num)
    write_csv(results, filename)

def trim_data(input_filename, output_filename):
    '''
    Find trim the data keep the time and location infor
    Input:
        input_filename: the raw csv file
        output_filename: name of the result file
    Return:
        No return, filter the data and store it to csv file
    '''
    raw = read_csv(input_filename)
    useful = raw.loc[:,['uniquekey','applicationtype',
        'applicationtype','applicationstartdate',
        'worktype', 'worktypedescription',
        'applicationfinalizeddate','parkingmeterpostingorbagging',
        'xcoordinate', 'ycoordinate', 'latitude',
        'longitude','location','streetclosure',
        'streetnumberfrom','streetnumberto',
        'direction','streetname']]
    output = DATA_DIR + output_filename
    useful.to_csv(output)


def read_csv(filename):
    '''
    Read data from the file 
    Return pd dataframe
    '''

    csv_file = DATA_DIR + filename
    data = pd.read_csv(csv_file)
    return data

def write_csv(results,filename):


    temp = set()
    for i in range(200):
        temp.update(set(results[i].keys()))
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