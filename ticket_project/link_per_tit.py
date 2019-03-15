'''
link two dataframe together
'''
import pandas as pd
import csv
import re
from datetime import datetime
import process_tickets as pro
import permits as per
import dask.dataframe as dd
import pdb
DATA_DIR = "./data/"

def link_permits_tickets(per_data, tit_data, output_file ='per_tit_whole.csv'):
    '''
    Join two dataframe together based on the location and issue time
    Input:
        output_file: filename of the big table

    Return:
        df: pandas dataframe contains the large joint table
    '''

    #per_data = pd.read_csv(DATA_DIR + 'clean_whole_permits.csv')


    tit_data['upper_streetname'] = tit_data['street_name'].apply(lambda m: get_name(m).group(1).upper()
        if get_name(m) else None)
    tit_data['upper_streetname'] = tit_data['upper_streetname'].astype('category')
    print(tit_data.info(memory_usage='deep'))
    #tit_data['street_name'] = tit_data['street_name'].astype(str)
    #tit_data['street_dir'] = tit_data['street_dir'].astype(str)

    per_data = per_data.rename(index = str, columns = {'streetname': 'upper_streetname', 'direction': 'street_dir'})
    

    tit_data = dd.from_pandas(tit_data, npartitions = 100)
    per_data = dd.from_pandas(per_data, npartitions = 40)
    combo = dd.merge(per_data, tit_data, on = ['upper_streetname','street_dir'])
    combo= combo[combo['streetnumberfrom'] <= combo['street_num']]
    combo= combo[combo['streetnumberto'] >= combo['street_num']]
    combo= combo[combo['issue_date'] >= combo['applicationstartdate']]
    combo= combo[combo['issue_date'] <= combo['applicationfinalizeddate']]

    combo = combo.compute()
    pdb.set_trace()
    combo.to_csv(DATA_DIR + output_file)

    print('done')

    return combo


def get_name(m):
    #pdb.set_trace()
    temp = re.match(r'(.+) (.+)$',m)
    if temp:
        return temp
    return None
