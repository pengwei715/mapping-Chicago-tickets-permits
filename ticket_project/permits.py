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
import pdb

DATA_DIR = "./data/"
TIT_DIR = DATA_DIR + 'tickets/'

COLUMNS = ['uniquekey',
           'applicationtype',
           'applicationstartdate',
           'worktype', 
           'worktypedescription',
           'applicationfinalizeddate',
           'latitude',
           'longitude',
           'streetclosure',
           'streetnumberfrom',
           'streetnumberto',
           'direction',
           'streetname']

MAXSIZE = 1041814

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
    results = client.get("erhc-fkv9",limit=MAXSIZE)
    df = pd.DataFrame.from_records(results)
    return df

def clean_permits(df, outfile):
    '''
    Clean the permits data
    Input:
        df: pandas dataframe get from web
    Return:
        cleaned dataframe 
    '''
    raw = dd.from_pandas(df, npartitions = 104)
    raw = raw.loc[:,COLUMNS]
      
    raw = raw.dropna(subset=['applicationstartdate', 
        'applicationfinalizeddate','streetnumberfrom', 'streetnumberto'])
    for item in ['applicationstartdate', 'applicationfinalizeddate']:
        raw[item] = dd.to_datetime(raw[item])
    for item in ['streetnumberfrom', 'streetnumberto']:
        raw[item] = raw[item].astype('int32') 
    clean = raw.compute()
    clean = clean[clean['streetclosure'].notna()]
    clean = clean[clean['streetclosure']!='None']
    clean = clean[clean['streetclosure']!='NA']
    clean.to_csv(DATA_DIR + outfile)
    return clean

def read(filename):
    return pd.read_csv(DATA_DIR +filename)