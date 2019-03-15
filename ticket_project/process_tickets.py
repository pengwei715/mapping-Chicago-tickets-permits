import pandas as pd 
import csv
import numpy
import gc


def import_tickets(ticket_file, dictionary_file):
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
    df = pd.read_csv(ticket_file, dtype=col_types, index_col='ticket_number',
		             usecols=col_types.keys())
    
    df['street_num'] = pd.to_numeric(df.street_num, downcast='unsigned')
    df['geocoded_lng'] = pd.to_numeric(df.geocoded_lng, downcast='float')
    df['geocoded_lat'] = pd.to_numeric(df.geocoded_lat, downcast='float')
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    violations = generate_code_dict(dictionary_file)
    df['violation_code'] = df['violation_code'].map(violations).astype('category')
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


def sweeping_data(df):
    return df[df['violation_description'] == 'STREET CLEANING'].drop(['zipcode', 'violation_description', 'violation_code'], axis=1)
