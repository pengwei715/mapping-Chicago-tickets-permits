import csv
import gc
import pandas as pd
import geocoder
import matplotlib
import geopandas
import neighborhoods as nbhds


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
    df['violation_code'] = \
                    df['violation_code'].map(violations).astype('category')
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
    '''
    link_with_neighborhoods is a helper function which processes a dataframe
    and returns a geocoded one
    input: (df) tickets_df
    output: (geo_df) geo_tickets_df
    '''
    nbhd = nbhds.import_geometries(nbhds.NEIGHS_ID)
    geodf = nbhds.convert_to_geodf(df, 'geocoded_lng', 'geocoded_lat')

    return nbhds.find_neighborhoods(geodf, nbhd)


def filter_input(df, input_dict):
    '''
    filter_input takes a tickets_df and returns a filtered df based on the
    filters given in input_dict

    inputs: (df) tickets_df
            (dict) input_dict
    returns: (df) filtered df
    '''
    fail_str = '{} {} not found, ignoring'
    success_str = 'filtered on {} number of tickets reduced from {} to {}'
    column_dict = {'violation': 'violation_code',
                   'start_date': 'issue_date',
                   'end_date': 'issue_date',
                   'location': ['geocoded_lng', 'geocoded_lat']}
    dist_diff = 0.0145 #approximately 1 mile in distance

    for key, val in input_dict.items():
        row_nums = df.shape[0]

        if key == 'violation':
            unique_vals = df[column_dict[key]].unique()
            if val not in unique_vals:
                print(fail_str.format(key, val))
            else:
                df = df[df[column_dict[key]] == val]
                print(success_str.format(val, row_nums, df.shape[0]))
        if key in ('start_date', 'end_date'):
            if key == 'start_date':
                df = df[df[column_dict[key]] > val]
            else:
                df = df[df[column_dict[key]] < val]
            print(success_str.format(val, row_nums, df.shape[0]))
        if key == 'location':
            g = geocoder.osm(input_dict[key])
            if g.x and g.y:
                mask = (df[column_dict[key][0]] < g.x + dist_diff) & \
                       (df[column_dict[key][0]] > g.x - dist_diff) & \
                       (df[column_dict[key][1]] < g.y + dist_diff) & \
                       (df[column_dict[key][1]] > g.y - dist_diff)
                df = df[mask]
                print(success_str.format(\
                    'locations within approxmiately one mile of ' + val, \
                    row_nums, df.shape[0]))
            else:
                print(fail_str.format(key, val))
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
    geocoded = link_with_neighborhoods(filtered)

    if 'location' in input_dict or 'neighborhood' in input_dict:
        filtered_nbhd = \
            nbhd[nbhd['pri_neigh'].isin(geocoded.pri_neigh.unique())]
        base = filtered_nbhd.plot(color='white', edgecolor='black')
        geocoded.plot(ax=base)

    else: #citywide
        base = nbhd.plot(color='white', edgecolor='black')
        heat = geocoded.dissolve(by='pri_neigh', aggfunc='count')
        heat.drop('coordinates', axis=1)
        heat = geopandas.GeoDataFrame(nbhd.join(heat, on='pri_neigh', \
            how='left', rsuffix='_heat'), geometry='the_geom', crs=nbhd.crs)
        heat.plot(ax=base, scheme='quantiles', column='issue_date', legend=True)

    matplotlib.pyplot.show()
