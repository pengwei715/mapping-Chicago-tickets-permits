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

def get_data():
    '''
    Get some sample permit data
    Input:
        num: number of rows 
        filename: the name of the csv file 
    Return:
        Pandas dataframe that contains all the data of permits.
    '''
    client = Socrata('data.cityofchicago.org',
                 'SB7994tcuBpSSczrQvMx9N0Uy',
                 username="benfogarty@uchicago.edu",
                 password="d5Nut6LrCHL&")
    results = client.get("erhc-fkv9",limit=1041814)
    return  pd.DataFrame.from_records(results)

def trim_data(df):
    '''
    Find trim the data keep the time and location infor
    Input:
        input_filename: the raw csv file
        output_filename: name of the result file
    Return:
        No return, filter the data and store it to csv file
    '''
    columns = ['uniquekey','applicationtype',
        'applicationstartdate',
        'worktype', 'worktypedescription',
        'applicationfinalizeddate','xcoordinate', 
        'ycoordinate', 'latitude',
        'longitude','location','streetclosure',
        'streetnumberfrom','streetnumberto',
        'direction','streetname']
    raw = df.loc[:,columns]
    raw = raw[raw['streetclosure'].notna()]
    clean = raw[raw['streetclosure']!='None']
    return clean

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

def select_by_year(filename, outputfile, year):

    file = DATA_DIR + filename

    raw = pd.read_csv(file)

    mask = raw['applicationstartdate'].str.contains(str(year),na = False)
    select = raw[mask]
    csv_file = DATA_DIR + outputfile
    select.to_csv(csv_file)
