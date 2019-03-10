import sys
import pandas as pd
import csv

def go(input_path, output_path):
    '''
    Takes the full ProPublica parking tickets dataset as a CSV, reads in columns of
    interest, filters out rows not covered by permits data, and outputs the
    reduced dataset and a map from violation descripitons to violation codes
    as a CSV file.

    Inputs:
        input_path (string): the path to the full dataset
        output_path (string): the folder for the reduced dataset and violation
            code dictionary to be created in
    '''
    cols = ['ticket_number', 'issue_date', 'violation_location',
            'zipcode', 'violation_code', 'violation_description',
            'geocoded_lng', 'geocoded_lat']
    col_types = {'ticket_number': str, 
                 'issue_date': str,
                 'violation_location': str,
                 'zipcode': 'category', 
                 'violation_code': str,
                 'violation_description': str,
                 'geocoded_lng': float,
                 'gecoded_lat': float}

    print('Reading in full dataset...')
    df = pd.read_csv(input_path, usecols=cols,
                     dtype=col_types, index_col='ticket_number')

    print('Filtering dataset...')
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    time_before = pd.to_datetime('1/1/2012 0:00')
    mask = df['issue_date'] > time_before
    df = df[mask]

    print('Collapsing violation columns...')
    with open(output_path + 'violations_dict.csv', 'w') as f:
        violation_codes = df.drop_duplicates(['violation_code',
                                              'violation_description'])\
                            .violation_description\
                            .to_csv(f)
    df = df.drop(['violation_description'], axis=1)

    print('Outputting reduced dataset...')
    with open(output_path + 'reduced_tickets.csv', 'w') as f:
        df.to_csv(f)



if __name__ == "__main__":
    usage = "python3 shrink_tickets.py <path to dataset> <output path>"
    assert (len(sys.argv) == 3), "Input and output path required."
    _, input_path, output_path = sys.argv
    if output_path[-1] != '/':
        output_path += '/'
    go(input_path, output_path)
