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


def search_for_patterns(df):
	sweeping = df[ df['violation_code'] == 'STREET CLEANING'].sort_values('issue_date')
	sweeping.groupby(sweeping['issue_date'].dt.month).count().issue_date
	sweeping.groupby(sweeping['issue_date'].dt.hour).count().issue_date

	resampled = sweeping.set_index('issue_date').resample('Y')

def find_similar_tickets(tickets_df, date_range, violation_code, location):
	'''
	find_similar_tickets takes a set of parameters in the following form
	and returns any tickets which match

	inputs: tickets_df (df): result of import_tickets
			date_range (tuple): ('YYYY-MM-DD', 'YYYY-MM-DD')
			violation_code (str): 'violation_code'
			location (tuple): (lat, lon)
	returns: df
	'''
	diff = 0.001
		   
	mask = (tickets_df['issue_date'] > date_range[0]) & \
		   (tickets_df['issue_date'] < date_range[1]) & \
		   (tickets_df['violation_code'] == violation_code) & \
		   (tickets_df['geocoded_lat'] < location[0] + diff) & \
		   (tickets_df['geocoded_lat'] > location[0] - diff) & \
		   (tickets_df['geocoded_lng'] < location[1] + diff) & \
		   (tickets_df['geocoded_lng'] > location[1] - diff)

	return tickets_df[mask]



