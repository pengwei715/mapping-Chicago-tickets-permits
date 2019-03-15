'''
use this to clean the streetclosure column
and link the data into tickets data
'''
import pandas as pd
import csv
import re
import pdb
from datetime import datetime
import dask.dataframe as dd #this library is used to deal with mutilthread processing
import select_data as se

DATA_DIR = "./data/"

def import_tickets(filename):
    '''
    Reads in a dataset of parking tickets, making some modifications to the
    format data is presented in to decrease the size of the dataframe in memory.

    Inputs:
    filename (str): the path to a tickets dataset

    Returns: Pandas dataframe
    '''
    col_types = {'ticket_number': str, 
                 'issue_date': str,
                 'violation_code': 'category',
                 'street_num': int,
                 'street_dir': 'category',
                 'street_name': 'category',
                 'zipcode': 'category',
                 'geocoded_lng': float,
                 'geocoded_lat': float}
    df = dd.read_csv(filename, dtype=col_types, index_col='ticket_number',
                     usecols=col_types.keys())
    
    df['street_num'] = pd.to_numeric(df.street_num, downcast='unsigned')
    df['geocoded_lng'] = pd.to_numeric(df.geocoded_lng, downcast='float')
    df['geocoded_lat'] = pd.to_numeric(df.geocoded_lng, downcast='float')
    df['issue_date'] = pd.to_datetime(df['issue_date'])

    return dd.compute()

def link_permits_tickets(output_file ='per_tit_whole.csv'):

    inp1 = DATA_DIR + inp1
    inp2 = DATA_DIR + inp2
    out = DATA_DIR + out

    tit = dd.read_csv(inp1, dtype ={'zipcode': str})
    per = dd.read_csv(inp2)

    per = per.drop(['Unnamed: 0','APPLICATIONTYPE.1', 'PARKINGMETERPOSTINGORBAGGING'], axis= 1)

    print('finish reading csv')


    tit['STREETNAME'] = tit['violation_location'].apply(lambda m: get_name(m).group(3)
        if get_name(m) else None)
    tit['DIRECTION'] =  tit['violation_location'].apply(lambda m: get_name(m).group(2)
        if get_name(m) else None)
    tit['t_streetnum'] = tit['violation_location'].apply(lambda m: get_name(m).group(1)
        if get_name(m) else None)
    tit['t_date'] = tit['issue_date'].apply(lambda m: get_date(m).group(0)
        if get_date(m) else None)

    tit = tit.dropna(subset=['t_streetnum', 't_date'])
    per = per.dropna(subset=['APPLICATIONSTARTDATE', 'APPLICATIONFINALIZEDDATE',
        'STREETNUMBERFROM', 'STREETNUMBERTO'])


    for col in ['STREETNUMBERFROM', 'STREETNUMBERTO']:
        per[col] = per[col].astype('int32')

    
    tit['t_streetnum'] = tit['t_streetnum'].astype('int32')
    tit['t_date'] = dd.to_datetime(tit['t_date'])

    for item in ['APPLICATIONSTARTDATE', 'APPLICATIONFINALIZEDDATE']:
        per[item] = dd.to_datetime(per[item])
    
    print('finish casting')
    combo = per.merge(tit, on = ['STREETNAME','DIRECTION'])
    print('finish merge')
    
    pdb.set_trace()

    combo= combo[combo['STREETNUMBERFROM'] <= combo['t_streetnum']]
    combo= combo[combo['STREETNUMBERTO'] >= combo['t_streetnum']]
    combo= combo[combo['t_date'] >= combo['APPLICATIONSTARTDATE']]
    combo= combo[combo['t_date'] <= combo['APPLICATIONFINALIZEDDATE']]

    print('finish filtering')

    combo = combo.drop(['Unnamed: 0','Unnamed: 0.1'], axis= 1)

    combo.compute().to_csv(out)

    print('done')

def get_name(m):
    temp = re.match(r'(\d+) ([A-Z]) (\w+)\s?(\w?)',m)
    if temp:
        return temp
    return None 
def get_date(m):
	temp = re.match(r'(\d{4}-\d{2}-\d{2})',m)
	if temp:
		return temp
	return None

