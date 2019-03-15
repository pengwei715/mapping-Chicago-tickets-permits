'''
link two dataframe together
'''
import pandas as pd
import csv
import re
from datetime import datetime
import process_tickets as pro
import permits as per
import pdb
DATA_DIR = "./data/"

def link_permits_tickets(tit_data, output_file ='per_tit_whole.csv'):
    '''
    Join two dataframe together based on the location and issue time
    Input:
        output_file: filename of the big table

    Return:
        df: pandas dataframe contains the large joint table
    '''
    pdb.set_trace()

    per_data = pd.read_csv(DATA_DIR + 'clean_whole_permits.csv')

    tit_data['street_name'] = tit_data['street_name'].astype(str)
    tit_data['street_dir'] = tit_data['street_dir'].astype(str)
    per_data = per_data.rename(index = str, columns = {'streetname': 'street_name', 'direction': 'street_dir'})
    combo = per_data.merge(tit_data, on = ['street_name','street_dir'])
    combo= combo[combo['streetnumberfrom'] <= combo['street_num']]
    combo= combo[combo['streetnumberto'] >= combo['street_num']]
    combo= combo[combo['issue_date'] >= combo['applicationstartdate']]
    combo= combo[combo['issue_date'] <= combo['applicationfinalizeddate']]

    combo.to_csv(DATA_DIR + output_file)

    print('done')

    return combo


def get_name(m):
    temp = re.match(r'(\d))