'''
Get the permit data from chicago data portal
Clean the data
'''
import pandas as pd
from sodapy import Socrata
import dask.dataframe as dd
import pdb

DATA_DIR = "./data/"
TIT_DIR = DATA_DIR + 'tickets/'

COLUMNS = ['uniquekey',
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

def get_permits(num=MAXSIZE):
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

    results = client.get("erhc-fkv9", limit=num)
    return pd.DataFrame.from_records(results)

def clean_permits():
    '''
    Clean the permits data
    Return:
        cleaned dataframe
    '''
    data = get_permits()
    raw = dd.from_pandas(data, npartitions=104)
    raw = raw.loc[:, COLUMNS]
    lst = ['applicationstartdate',
           'applicationfinalizeddate',
           'streetnumberfrom',
           'streetnumberto']
    raw = raw.dropna(subset=lst)

    for item in ['applicationstartdate', 'applicationfinalizeddate']:
        raw[item] = dd.to_datetime(raw[item])

    for item in ['streetnumberfrom', 'streetnumberto']:
        raw[item] = raw[item].astype('int32')

    clean = raw.compute()
    clean = clean[clean['streetclosure'].notna()]
    clean = clean[clean['streetclosure'] != 'None']
    clean = clean[clean['streetclosure'] != 'NA']

    return clean
