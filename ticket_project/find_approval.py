'''
use this to clean the streetclosure column
and link the data into tickets data
'''
import pandas as pd
import csv
import re
import pdb
from datetime import datetime

DATA_DIR = "./data/"

def filter_permits(inp='permits_2015.csv', out= 'closed_permits_2015.csv'):
    '''
    drop the streetclosure data where is NA or None
    '''
    csv_file = DATA_DIR + inp
    out = DATA_DIR + out
    data = pd.read_csv(csv_file)
    pdb.set_trace()
    raw = data[data['STREETCLOSURE'].notna()]
    clean = raw[raw['STREETCLOSURE']!='None']
    clean.to_csv(out)

def link_permits_tickets(inp1='tickets_2015.csv', 
    inp2= 'closed_permits_2015.csv', out='link_p_t_2015.csv'):

    inp1 = DATA_DIR + inp1
    inp2 = DATA_DIR + inp2
    out = DATA_DIR + out
    cols = ['ticket_number', 'issue_date', 'violation_location',
        'zipcode', 'violation_code', 'violation_description',
        'total_payments', 'address']

    tit = pd.read_csv(inp1)
    tit = tit.loc[:,cols]
    per = pd.read_csv(inp2)

    tit['STREETNAME'] = tit['violation_location'].apply(lambda m: get_name(m).group(3)
        if get_name(m) else None)
    tit['DIRECTION'] =  tit['violation_location'].apply(lambda m: get_name(m).group(2)
        if get_name(m) else None)
    tit['t_streetnum'] = tit['violation_location'].apply(lambda m: get_name(m).group(1)
        if get_name(m) else None)
    tit['t_date'] = tit['issue_date'].apply(lambda m: get_date(m).group(0)
        if get_date(m) else None)
    combo = per.merge(tit, on = ['STREETNAME','DIRECTION'])

    for col in ['t_streetnum', 'STREETNUMBERFROM', 'STREETNUMBERTO']:
    	combo[col] = combo[col].astype('int32')

    combo['APPLICATIONSTARTDATE'] = pd.to_datetime(combo['APPLICATIONSTARTDATE'])
    combo['APPLICATIONFINALIZEDDATE'] = pd.to_datetime(combo['APPLICATIONFINALIZEDDATE'])
    combo['t_date'] = pd.to_datetime(combo['t_date'])

    combo= combo[combo['STREETNUMBERFROM'] <= combo['t_streetnum']]
    combo= combo[combo['STREETNUMBERTO'] >= combo['t_streetnum']]
    combo= combo[combo['t_date'] >= combo['APPLICATIONSTARTDATE']]
    combo= combo[combo['t_date'] <= combo['APPLICATIONFINALIZEDDATE']]

    in_street = combo
    in_street = in_street.drop(['Unnamed: 0','Unnamed: 0.1', 'Unnamed: 0.1.1'], axis= 1)
    in_street.to_csv(out)

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

