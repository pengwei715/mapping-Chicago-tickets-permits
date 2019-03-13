import pandas as pd 
#import geocoder
import csv

def reduce_df(df, output_path):
    print('Collapsing violation columns')
    violation_dict = {}
    for name, grouped_df in df.groupby(['violation_description']):
        violation_dict[name] = grouped_df['violation_code'].iloc[1]
    code_path = output_path[:-4] + 'coded_violations.csv'
    with open(code_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in violation_dict.items():
            writer.writerow([key, value])
    df = df.drop(['violation_description'], axis=1)

    print('Outputting reduced dataset...')
    df.to_csv(output_path)




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
                 'geocoded_address': str,
		         'geocoded_lng': float,
		         'geocoded_lat': float}
    df = pd.read_csv(filename, dtype=col_types, index_col='ticket_number',
		             usecols=col_types.keys())
    print('here')
    address_split = df.geocoded_address.str.split(\
		            r'([0-9]+)\s+([NSEW])\s(.+),.+,.+([0-9]{5,5})',\
                    expand=True)
    df['street_num'] = address_split.iloc[:,[1]].astype(str)
    df['street_dir'] = address_split.iloc[:,[2]]
    df['street_dir'] = df['street_dir'].astype('category')
    df['street_name'] = address_split.iloc[:,[3]]
    df['street_name'] = df['street_name'].astype('category')
    df['zipcode'] = address_split.iloc[:,[4]]
    df['zipcode'] = df['zipcode'].astype('category')
    df.drop(labels='geocoded_address', axis=1, inplace=True)
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    return df

def sweeping_data(df):
    return df[df['violation_description'] == 'STREET CLEANING'].drop(['zipcode', 'violation_description', 'violation_code'], axis=1)
