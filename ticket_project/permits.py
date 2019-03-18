'''
Get the permit data from chicago data portal
Clean the data
'''
import pandas as pd
from sodapy import Socrata
import numpy as np
from datetime import datetime

MAXSIZE = 1041814 #whole size of data

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

    conds = '''{} IS NOT NULL AND
        {} != "NA" AND 
        {} != "None" AND
        {} IS NOT NULL AND
        {} IS NOT NULL AND
        {} IS NOT NULL AND
        {} IS NOT NULL'''\
        .format('streetclosure',
        'streetclosure',
        'streetclosure',
        'applicationstartdate',
        'streetnumberfrom',
        'streetnumberto',
        'applicationenddate')

    res = client.get("erhc-fkv9", 
                    select=','.join(coltypes.keys()), 
                    where=conds, 
                    limit=MAXSIZE)

    df = pd.DataFrame.from_records(res)\
                     .astype(coltypes)
    
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

