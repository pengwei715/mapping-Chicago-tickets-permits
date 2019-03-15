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
import sys
import pandas as pd
import requests
import json
import csv
from sodapy import Socrata
import re
from datetime import datetime
import dask.dataframe as dd

DATA_DIR = "./data/"
TIT_DIR = DATA_DIR + '/tickets'

COLUMNS = ['uniquekey',
           'applicationtype',
           'applicationstartdate',
           'worktype', 
           'worktypedescription',
           'applicationfinalizeddate',
           'xcoordinate', 
           'ycoordinate', 
           'latitude',
           'longitude',
           'location',
           'streetclosure',
           'streetnumberfrom',
           'streetnumberto',
           'direction',
           'streetname']

def get_permits():
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
    df = pd.DataFrame.from_records(results)
    return df

def clean_permits(df):
    raw = dd.from_pandas(df, npartitions = 104)
    raw = raw.loc[:,COLUMNS]
    raw = raw[raw['streetclosure'].notna()]
    raw = raw[raw['streetclosure']!='None']   
    raw = raw.dropna(subset=['applicationstartdate', 
        'applicationfinalizeddate','streetnumberfrom', 'streetnumberto'])
    for item in ['applicationstartdate', 'applicationfinalizeddate']:
        raw[item] = dd.to_datetime(per[item])   
    return raw.compute()

def split_tickets(inp='reduced_tickets.csv', out='tickets'): 

  
    inp = DATA_DIR + inp
    raw =  pd.read_csv(inp, low_memory=False)
    mask_2015 = raw['issue_date'].str.contains('2015',na =False)
    mask_2016 = raw['issue_date'].str.contains('2016',na =False)
    mask_2017 = raw['issue_date'].str.contains('2017',na =False)
    mask_2018 = raw['issue_date'].str.contains('2018',na =False)

    raw_2015 = raw[mask_2015]
    raw_2015.to_csv(out + '_2015.csv')
    raw_2016 = raw[mask_2016]
    raw_2016.to_csv(out + '_2016.csv')
    raw_2017 = raw[mask_2017]
    raw_2017.to_csv(out + '_2017.csv')
    raw_2018 = raw[mask_2018]
    raw_2018.to_csv(out + '_2018.csv')
    

    result = [raw_2015, raw_2016, raw_2017, raw_2018]
    whole = pd.concat(result)
    whole.to_csv(DATA_DIR + out + '_15_18.csv')
    return whole










'''

def read_csv(filename):
    Read data from the file 
    Return pd dataframe
    csv_file = DATA_DIR + filename
    data = pd.read_csv(csv_file)
    return data

def write_csv(results, filename):

    csv_file = DATA_DIR + filename
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=COLUMNS)
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
'''
