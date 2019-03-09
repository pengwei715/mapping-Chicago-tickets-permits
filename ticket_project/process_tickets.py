import pandas as pd 
import geocoder

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

