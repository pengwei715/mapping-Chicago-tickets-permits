import sys
import pandas as pd

def go(input_path, output_path):
    '''
    Takes the full ProPublica parking tickets dataset as a CSV, reads in columns of
    interest, filters out rows not covered by permits data, and outputs the
    reduced dataset as a CSV file.

    Inputs:
        input_path (string): the path to the full dataset
        output_path (string): the path for the reduced dataset to be created in
    '''
    cols = ['ticket_number', 'issue_date', 'violation_location',
            'zipcode', 'violation_code', 'violation_description',
            'total_payments', 'address']
    col_types = {'ticket_number': str, 
                 'issue_date': str,
                 'violation_location': str,
                 'zipcode': 'category', 
                 'violation_code': 'category',
                 'violation_description': 'category',
                 'total_payments': float,
                 'address': str}

    print('Reading in full dataset...')
    df = pd.read_csv(input_path, usecols=cols,
                     dtype=col_types, index_col='ticket_number')

    print('Filtering dataset...')
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    time_before = pd.to_datetime('1/1/2012 0:00')
    mask = df['issue_date'] > time_before
    df = df[mask]

    print('Collapsing violation columns')
    violation_dict = {}
    for name, grouped_df in df.groupby(['violation_description']):
        violation_dict[name] = grouped_df['violation_code'].iloc[1]
    code_path = output_path[:-4] + '_coded_violations.csv'
    with open(code_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in violation_dict.items():
            writer.writerow([key, value])
    df = df.drop(['violation_description'], axis=1)

    print('Outputting reduced dataset...')
    df.to_csv(output_path)



if __name__ == "__main__":
    usage = "python3 shrink_tickets.py <path to dataset> <output path>"
    assert (len(sys.argv) == 3), "Input and output path required."
    _, input_path, output_path = sys.argv
    go(input_path, output_path)