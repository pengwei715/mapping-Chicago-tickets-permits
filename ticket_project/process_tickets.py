import pandas as pd 
import csv
import numpy
import gc
import neighborhoods as nbhds
import geocoder
import math
import matplotlib
import mapclassify

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

def link_with_neighborhoods(df):
    nbhd = nbhds.import_geometries(nbhds.NEIGHS_ID)
    geodf = nbhds.convert_to_geodf(df, 'geocoded_lng', 'geocoded_lat')

    return nbhds.find_neighborhoods(geodf, nbhd)


def search_for_patterns(df):
    sweeping = df[ df['violation_code'] == 'STREET CLEANING'].sort_values('issue_date')
    sweeping.groupby(sweeping['issue_date'].dt.month).count().issue_date
    sweeping.groupby(sweeping['issue_date'].dt.hour).count().issue_date

    resampled = sweeping.set_index('issue_date').resample('Y')

def filter_input(df, input_dict):
    fail_str = '{} {} not found, ignoring'
    success_str = 'filtered on {} number of tickets reduced from {} to {}'
    column_dict = {'neighborhood': 'pri_neigh',
                   'violation': 'violation_code',
                   'start_date': 'issue_date',
                   'end_date': 'issue_date',
                   'location': ['geocoded_lng', 'geocoded_lat']}
    dist_diff = 0.0145 #approximately 1 mile in distance

    for key in input_dict:
        row_nums = df.shape[0]
        if key in ('neighborhood', 'violation'):
            unique_vals = df[column_dict[key]].unique()
            input_val = input_dict[key]
            
            if input_val not in unique_vals:
                print(fail_str.format(key, input_val))
            else:
                df = df[ df[column_dict[key]] == input_val ]
                print(success_str.format(input_val, row_nums, df.shape[0]))
        if key in ('start_date', 'end_date'):
            date_input = input_dict[key]
            if key == 'start_date':
                df = df[ df[column_dict[key]] > date_input]
            else:
                df = df[ df[column_dict[key]] < date_input]
            print(success_str.format(date_input, row_nums, df.shape[0]))
        if key == 'location':
            g = geocoder.osm(input_dict[key])
            input_val = input_dict[key]
            if g.x and g.y:
                mask = (df[column_dict[key][0]] < g.x + dist_diff) & \
                       (df[column_dict[key][0]] > g.x - dist_diff) & \
                       (df[column_dict[key][1]] < g.y + dist_diff) & \
                       (df[column_dict[key][1]] > g.y - dist_diff)

                df = df[mask]
                print(success_str.format('locations within approxmiately one mile of ' + input_val, row_nums, df.shape[0]))
            else:
                print(fail_str.format(key, input_val))
    return df


def find_similar_tickets(tickets_df, input_dict):
    '''
    find_similar_tickets takes a set of parameters in the following form
    and returns any tickets which match

    inputs: tickets_df (df): result of import_tickets
            input_dict (dict): a dictionary which includes various filters
                               including...
                               'neighborhood' : 'neighborhood'
                               'start_date' : 'YYYY-MM_DD'
                               'end_date' : 'YYYY-MM_DD'
                               'location' : ex. 'number street Chicago, IL'
                               'violation' : 'violation_code'
    returns: df
    '''
    filtered = filter_input(tickets_df, input_dict)

    nbhd = nbhds.import_geometries(nbhds.NEIGHS_ID)
    base = nbhd.plot(color='white', edgecolor='black')
    heat = filtered.dissolve(by='pri_neigh', aggfunc='count')
    heat.drop('coordinates', axis=1)
    #join on neighborhoods

    heat.join()
    heat.plot(ax=base, scheme='quantiles', column='issue_date')

    #filtered.plot(ax=base)

    matplotlib.pyplot.show()



    return filtered
    
