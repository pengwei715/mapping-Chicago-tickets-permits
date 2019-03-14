import pandas as pd 
import geocoder
import csv
import numpy
import gc

def geocode(series):
	'''
	geocode takes a series from a groupby.size()
	'''
	geodict = {}
	not_coded = {}
	i = 0
	for add, amt in series.iteritems():
		g = geocoder.osm(add)
		if g.x:
			geodict[add] = {'x': g.x, 'y': g.y, 'amt': amt}
		else:
			not_coded[add] = {'amt': amt}
		if i % 10 == 0:
			print(i)
	return geodict, not_coded

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
    df = pd.read_csv(filename, dtype=col_types, index_col='ticket_number',
		             usecols=col_types.keys())
    
    df['street_num'] = pd.to_numeric(df.street_num, downcast='unsigned')
    df['geocoded_lng'] = pd.to_numeric(df.geocoded_lng, downcast='float')
    df['geocoded_lat'] = pd.to_numeric(df.geocoded_lng, downcast='float')
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    gc.collect()

    return df

def sweeping_data(df):
    return df[df['violation_description'] == 'STREET CLEANING'].drop(['zipcode', 'violation_description', 'violation_code'], axis=1)
