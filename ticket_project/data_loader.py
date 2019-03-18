'''
data_loader stores helper functions which convert
raw datasets into specific forms so that they can be
analyzed
'''
import csv
import gc
from datetime import datetime
import pandas as pd
from sodapy import Socrata
import numpy as np


MAXSIZE = 1041814 #whole size of data
TICKETS_FILEPATH = 'data/reduced_tickets.csv'
VIOLATIONS_FILEPATH = 'data/violations_dict.csv'

def import_tickets(ticket_file, dictionary_file):
    '''
    Reads in a dataset of parking tickets, making some modifications to the
    format data is presented in to decrease the size of the dataframe in memory.

    Inputs:
    filename (str): the path to a tickets dataset

    Returns: Pandas dataframe

    References:
    Reducing dataframe memory usage: https://www.dataquest.io/blog/
        pandas-big-data
    '''
    col_types = {'ticket_number': str,
                 'issue_date': str,
                 'violation_code': 'category',
                 'street_num': int,
                 'street_dir': 'category',
                 'street_name': 'category',
                 'zipcode': 'category',
                 'geocoded_lng': float,
                 'geocoded_lat': float,
                 'fine_amt': float}
    df = pd.read_csv(ticket_file, dtype=col_types, index_col='ticket_number',
                     usecols=col_types.keys())

    df['street_num'] = pd.to_numeric(df.street_num, downcast='unsigned')
    df['geocoded_lng'] = pd.to_numeric(df.geocoded_lng, downcast='float')
    df['geocoded_lat'] = pd.to_numeric(df.geocoded_lat, downcast='float')
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    df['fine_amt'] = pd.to_numeric(df.fine_amt, downcast='float')
    violations = generate_code_dict(dictionary_file)
    df['violation_code'] = \
                    df['violation_code'].map(violations).astype('category')
    gc.collect()

    return df

def generate_code_dict(filename):
    '''
    Reads in a csv of violation codes and violation names and
    produces a dictionary which maps codes to names
    '''
    d = {}
    with open(filename, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        for key, value in reader:
            d[key] = value
    return d

def get_permits(start_date):
    '''
    Get whole sample permit data
    Input:
        start_date: (str) 'MM-DD-YYYY' date after which to capture
        permits data; first date with data is '07-13-2015'
    Return:
        Pandas dataframe that contains all useful data of permits.
    '''
    coltypes = {'applicationstartdate': str,
                'applicationexpiredate': str,
                'applicationenddate': str,
                'applicationfinalizeddate': str,
                'worktype': 'category',
                'worktypedescription': str,
                'latitude': float,
                'longitude': float,
                'streetclosure': 'category',
                'streetnumberfrom': int,
                'streetnumberto': int,
                'direction': 'category',
                'streetname': 'category'}

    client = Socrata('data.cityofchicago.org',
                     'SB7994tcuBpSSczrQvMx9N0Uy',
                     username="benfogarty@uchicago.edu",
                     password="d5Nut6LrCHL&")
    conds_str = '''{} IS NOT NULL AND
                   {} != "NA" AND 
                   {} != "None" AND
                   {} IS NOT NULL AND
                   {} IS NOT NULL AND
                   {} IS NOT NULL AND
                   {} IS NOT NULL'''

    conds = conds_str.format('streetclosure', 'streetclosure', 'streetclosure',\
                             'applicationstartdate', 'streetnumberfrom',\
                             'streetnumberto', 'applicationenddate')

    res = client.get("erhc-fkv9", select=','.join(coltypes.keys()),\
                     where=conds, limit=MAXSIZE)

    df = pd.DataFrame.from_records(res).astype(coltypes)

    client.close()

    date_cols = ['applicationstartdate', 'applicationexpiredate',
                 'applicationfinalizeddate', 'applicationenddate']

    for item in date_cols:
        df[item] = pd.to_datetime(df[item])

    df['applicationexpiredate'] = np.where(df.applicationexpiredate.isnull(),
                                           df.applicationenddate,
                                           df.applicationexpiredate)
    df['applicationexpiredate'] = np.where(df.applicationfinalizeddate < \
                                           df.applicationexpiredate,
                                           df.applicationfinalizeddate,
                                           df.applicationexpiredate)
    df = df.drop(['applicationenddate', 'applicationfinalizeddate'], axis=1)
    cutoff = datetime.strptime(start_date, '%m-%d-%Y')
    return df[df.applicationexpiredate >= cutoff]
