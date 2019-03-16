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

COLUMNS = ['applicationstartdate',
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
    col_types = {'applicationstartdate': str,
                 'applicationfinalizeddate': str,
                 'worktype': 'category',
                 'streetnumberfrom': int,
                 'streetnumberto':int,
                 'streetclosure': 'category',
                 'streetname': 'category',
                 'latitude': float,
                 'longitude': float}

    client = Socrata('data.cityofchicago.org',
    	             'SB7994tcuBpSSczrQvMx9N0Uy',
                     username="benfogarty@uchicago.edu",
                     password="d5Nut6LrCHL&")
    pdb.set_trace()
    results = client.get("erhc-fkv9", limit=num)
    return pd.DataFrame.from_records(results, dtypes=col_types)

def clean_permits():
    '''
    Clean the permits data
    Return:
        cleaned dataframe
    '''
    data = get_permits()
    data['streetnumberfrom'] = pd.to_numeric(data.streetnumberfrom, downcast='unsigned')
    data['streetnumberto'] = pd.to_numeric(data.streetnumberto, downcast='unsigned')
    data['latitude'] = pd.to_numeric(df.latitude, downcast='float')
    data['longitude'] = pd.to_numeric(df.longitude, downcast='float')

    raw = dd.from_pandas(data, npartitions=104)
    raw = raw.loc[:, COLUMNS]
    lst = ['applicationstartdate',
           'applicationfinalizeddate',
           'streetnumberfrom',
           'streetnumberto']
    raw = raw.dropna(subset=lst)

    for item in ['applicationstartdate', 'applicationfinalizeddate']:
        raw[item] = dd.to_datetime(raw[item])

    clean = raw.compute()
    clean = clean[clean['streetclosure'].notna()]
    clean = clean[clean['streetclosure'] != 'None']
    clean = clean[clean['streetclosure'] != 'NA']

    return clean
