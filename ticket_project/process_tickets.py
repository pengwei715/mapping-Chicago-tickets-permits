import pandas as pd 
import geocoder
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
    col_types = {'ticket_number': str, 
                 'issue_date': str,
                 'violation_location': str,
                 'zipcode': 'category', 
                 'violation_code': 'category',
                 'violation_description': 'category',
                 'total_payments': float,
                 'address': str}
    df = pd.read_csv(filename, dtype=col_types, index_col='ticket_number')
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    return df

def sweeping_data(df):
    return df[df['violation_description'] == 'STREET CLEANING'].drop(['zipcode', 'violation_description', 'violation_code'], axis=1)