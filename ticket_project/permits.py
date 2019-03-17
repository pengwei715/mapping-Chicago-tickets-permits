'''
Get the permit data from chicago data portal
Clean the data
'''
import pandas as pd
from sodapy import Socrata
import numpy as np

MAXSIZE = 1041814 #whole size of data

def get_permits(num=MAXSIZE):
    '''
    Get whole sample permit data
    Input:
        num: number of rows
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
        {} IS NOT NULL '''\
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
                    limit=num)

    df = pd.DataFrame.from_records(res)\
                     .astype(coltypes)
    
    client.close()

    date_cols = ['applicationstartdate', 'applicationexpiredate', 
                 'applicationfinalizeddate', 'applicationenddate']

<<<<<<< HEAD
    for item in date_cols:
        df[item] = pd.to_datetime(df[item])
    
=======
    for item in ['applicationstartdate', 
                 'applicationexpiredate', 
                 'applicationfinalizeddate', 
                 'applicationenddate']:
        df[item] = pd.to_datetime(df[item])

>>>>>>> fcf667ea453d00726e3ebbea7637b5dfb348a12c
    df['applicationexpiredate'] = np.where(df.applicationexpiredate.isnull(),
                                           df.applicationenddate, 
                                           df.applicationexpiredate)
    df['applicationexpiredate'] = np.where(df.applicationfinalizeddate < \
                                           df.applicationexpiredate,
                                           df.applicationfinalizeddate, 
                                           df.applicationexpiredate)
    df = df.drop(['applicationenddate', 'applicationfinalizeddate'], axis=1)

    return df

