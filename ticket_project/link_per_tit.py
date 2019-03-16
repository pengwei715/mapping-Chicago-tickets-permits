'''
link two dataframe together
'''
import re
import pandas as pd
import numpy as np
import dask.dataframe as dd

DATA_DIR = "./data/"

def link_permits_tickets(per_data, tit_data, output_file='per_tit_whole.csv'):
    '''
    Join two dataframe together based on the location and issue time
    Input:
        output_file: filename of the big table

    Return:
        df: pandas dataframe contains the large joint table
    '''

    abstract = lambda m: get_name(m).group(1).upper() if get_name(m) else None

    tit_data['upper_streetname'] = tit_data['street_name'].apply(abstract)
    tit_data['upper_streetname'] = tit_data['upper_streetname'].astype('category')

    per_data = per_data.rename(index=str,\
        columns={'streetname': 'upper_streetname', 'direction': 'street_dir'})

    per_data['streetnumberfrom'] = np.floor(per_data['streetnumberfrom']/100) * 100
    per_data['streetnumberto'] = np.ceil(per_data['streetnumberto']/100) * 100

    tit_data = dd.from_pandas(tit_data, npartitions=100)
    per_data = dd.from_pandas(per_data, npartitions=40)

    combo = dd.merge(per_data, tit_data, on=['upper_streetname', 'street_dir'])
    combo = combo[combo['streetnumberfrom'] <= combo['street_num']]
    combo = combo[combo['streetnumberto'] >= combo['street_num']]
    combo = combo[combo['issue_date'] >= combo['applicationstartdate']]
    combo = combo[combo['issue_date'] <= combo['applicationfinalizeddate']]

    combo = combo.compute()
    combo.to_csv(DATA_DIR + output_file)
    print('done')

    return combo

def get_name(string):
    '''
    Helper function to get the streetname

    Input:
        m: street_name string from tickets
    Return:
        match object
    '''
    match = re.match(r'(.+) (.+)$', string)
    return match if match else None
