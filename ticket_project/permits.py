'''
Get the permit data from chicago data portal
Clean the data
'''
import pandas as pd
from sodapy import Socrata

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

MAXSIZE = 1041814 #whole size of data

def get_permits(num=MAXSIZE):
    '''
    Get some sample permit data
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
             {} IS NOT NULL'''\
             .format('streetclosure',
                'streetclosure',
                'streetclosure',
                'applicationstartdate',
                'applicationfinalizeddate',
                'streetnumberfrom',
                'streetnumberto')

    res = client.get("erhc-fkv9", 
        select=','.join(COLUMNS), where=conds, limit=num)
    df = pd.DataFrame.from_records(res)
    
    client.close()

    for item in ['streetnumberfrom', 'streetnumberto']:
        df[item] = pd.to_numeric(df[item], downcast='unsigned')

    for item in ['latitude', 'longitude']:
        df[item] = pd.to_numeric(df[item], downcast='float')

    for item in ['applicationstartdate', 'applicationfinalizeddate']:
        df[item] = pd.to_datetime(df[item])

    for item in ['streetclosure', 'streetname']:
        df[item] = df[item].astype('category')

    return df

