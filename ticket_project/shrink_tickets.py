'''
Shrink the tickets data set
'''
import sys
import pandas as pd

def go(input_path, output_path):
    '''
    Takes the full ProPublica parking tickets dataset as a CSV, reads in columns
    of interest, filters out rows not covered by permits data, and outputs the
    reduced dataset and a map from violation descripitons to violation codes
    as a CSV file.

    Inputs:
        input_path (string): the path to the full dataset
        output_path (string): the folder for the reduced dataset and violation
            code dictionary to be created in
    '''
    cols = ['ticket_number', 'issue_date', 'violation_code',
            'violation_description', 'geocoded_address', 'geocoded_lng',
            'geocoded_lat', 'fine_level1_amount']
    col_types = {'ticket_number': str,
                 'issue_date': str,
                 'violation_code': str,
                 'violation_description': str,
                 'geocoded_address': str,
                 'geocoded_lng': float,
                 'gecoded_lat': float,
                 'fine_level1_amount': float}

    print('Reading in full dataset...')
    df = pd.read_csv(input_path, usecols=cols,
                     dtype=col_types, index_col='ticket_number')
    df = df.rename({'fine_level1_amount': 'fine_amt'}, axis=1)
    df = filter_by_row(df)
    df = collapse_violations_columns(df, output_path)
    df = convert_address_column(df)

    output_data(df, output_path)


def filter_by_row(df):
    '''
    Removes rows from the dataset occurring before 13 July 2015, as this time
    may be inconsistently covered in the permits dataset.

    Inputs:
    df (Pandas dataframe): the dataset of tickets

    Returns: Pandas dataframe
    '''
    print('Filtering dataset...')
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    time_before = pd.to_datetime('7/13/2015 0:00')
    mask = df['issue_date'] > time_before
    df = df.loc[mask]

    return df

def collapse_violations_columns(df, output_path):
    '''
    Removes the violations description column and generates a dictionary linking
    violation codes to violation descriptions. The dictonary is saved to a csv
    file in the output path.

    Inputs:
    df (Pandas dataframe): the dataset of tickets
    output_path (string): the folder for the violation code dictionary to be
        created in

    Returns: Pandas dataframe
    '''
    print('Collapsing violation columns...')
    with open(output_path + 'violations_dict.csv', 'w') as f:
        violation_codes = df.drop_duplicates(['violation_code',
                                              'violation_description'])
        violation_codes = violation_codes.drop(['issue_date',
                                                'geocoded_address',
                                                'geocoded_lng',
                                                'geocoded_lat',
                                                'fine_amt'], axis=1)
        violation_codes.to_csv(f, index=False)

    df = df.drop(['violation_description'], axis=1)

    return df

def convert_address_column(df):
    '''
    Converts the address column to three columns, the street number, the street
    direction, the street name, and the zipcode. Removes any rows for which the
    geocoded address cannot be parsed (which genrally means that the location
    was not accurately geocoded or that the location is outside of Chicago).

    Inputs:
    df (Pandas dataframe): the dataset of tickets

    Returns: Pandas dataframe
    '''
    print('Converting address columns...')
    address_split = df.geocoded_address.str.extract(\
                    r'([0-9]+)\s+([NSEW])\s(.+),.+,.+([0-9]{5,5})',\
                    expand=True)
    df = pd.concat([df, address_split], axis=1)
    rename_cols = {0: 'street_num',
                   1: 'street_dir',
                   2: 'street_name',
                   3: 'zipcode'}
    df = df.rename(rename_cols, axis=1)
    df = df.drop(labels='geocoded_address', axis=1)

    print('Removing rows with unparsable address data...')
    df = df[df.zipcode.notna() & df.street_num.notna()\
            & df.street_dir.notna()]

    return df

def output_data(df, output_path):
    '''
    Outputs the reduced tickets dataset and six samples of the dataset with
    sizes 10^n for n between 1 and 6 to csv files in the output path.

    Inputs:
    output_path (string): the folder for the reduced dataset and violation
    code dictionary to be created in
    '''
    print('Outputting reduced dataset...')
    with open(output_path + 'reduced_tickets.csv', 'w') as f:
        df.to_csv(f)

    print('Generating and outputting samples...')
    for i in range(1, 7):
        n = 10 ** i
        if n <= len(df):
            output_loc = output_path + 'sample_tickets_' + str(n) + '.csv'
            with open(output_loc, 'w') as f:
                df.sample(n).to_csv(f)
        else:
            break

if __name__ == "__main__":
    usage = "python3 shrink_tickets.py <path to dataset> <output path>"
    assert (len(sys.argv) == 3), "Input and output path required."
    _, input_path, output_path = sys.argv
    if output_path[-1] != '/':
        output_path += '/'
    go(input_path, output_path)
