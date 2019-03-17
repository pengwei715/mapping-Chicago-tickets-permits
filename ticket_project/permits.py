'''
Get the permit data from chicago data portal
Clean the data
'''
import pandas as pd
from sodapy import Socrata
import numpy as np

COLUMNS = ['applicationstartdate',
           'worktype',
           'applicationfinalizeddate',
           'worktypedescription',
           'applicationexpiredate',
           'latitude',
           'longitude',
           'streetclosure',
           'streetnumberfrom',
           'streetnumberto',
           'direction',
           'streetname', 
           'applicationenddate']

MAXSIZE = 1041814 #whole size of data

def get_permits(num=MAXSIZE):
    '''
    Get whole sample permit data
    Input:
        num: number of rows
    Return:
        Pandas dataframe that contains all useful data of permits.
    '''
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
                    select=','.join(COLUMNS), 
                    where=conds, 
                    limit=num)

    df = pd.DataFrame.from_records(res)
    
    client.close()

    for item in ['streetnumberfrom', 'streetnumberto']:
        df[item] = pd.to_numeric(df[item], downcast='unsigned')

    for item in ['latitude', 'longitude']:
        df[item] = pd.to_numeric(df[item], downcast='float')

    for item in ['applicationstartdate', 
                 'applicationexpiredate', 
                 'applicationfinalizeddate', 
                 'applicationenddate']:
        df[item] = pd.to_datetime(df[item])

    df['applicationexpiredate'] = np.where(df.applicationexpiredate.isnull(),
                                           df.applicationenddate, 
                                           df.applicationexpiredate)
    df.drop('applicationexpiredate', axis=1)
    df['applicationexpiredate'] = np.where(df.applicationfinalizeddate < \
                                           df.applicationexpiredate,
                                           df.applicationfinalizeddate, 
                                           df.applicationexpiredate)
    df.drop('applicationfinalizeddate', axis=1)

    for item in ['streetclosure', 'streetname']:
        df[item] = df[item].astype('category')

    return df

