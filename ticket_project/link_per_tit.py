'''
link two dataframe together
We only care about the violation_code 'PARKING/STANDING PROHIBITED ANYTIME'
with full streetclosure during the permits time and in the permits area
'''

import pandas as pd
import re
from datetime import datetime
import numpy as np
import gc

def link_permits_tickets(per, tik):
    '''
    Join two dataframe together based on the location and issue time
    
    Input:
        output_file: filename of the big table
    Return:
        df: pandas dataframe contains the large joint table
    '''
    per = per[per['streetclosure'] == 'Full']
    tik = tik[tik['violation_code'] == 'NO STANDING/PARKING TIME RESTRICTED']
    tik['upper_streetname'] = tik.street_name.str.extract(r'(.+)\s.+\Z',
                                                          expand=True)
    tik['upper_streetname'] = tik.upper_streetname.str.upper()
    tik['upper_streetname'] = tik['upper_streetname'].astype('category')
    print(len(tik))
    print(len(per))

    per = per.rename(index = str, columns = {'streetname': 'upper_streetname', 
                                             'direction': 'street_dir'})
    
    per.loc[:,'streetnumberfrom'] = np.floor(per['streetnumberfrom']/100)*100
    per.loc[:,'streetnumberto'] = np.ceil(per['streetnumberto']/100)*100

    gc.collect()
    tik = tik.reset_index()
    combo = tik.merge(per, on = ['upper_streetname','street_dir'])\
               .drop_duplicates('ticket_number')
    combo= combo[combo['streetnumberfrom'] <= combo['street_num']]
    combo= combo[combo['streetnumberto'] >= combo['street_num']]
    combo= combo[combo['issue_date'] >= combo['applicationstartdate']]
    combo= combo[combo['issue_date'] <= combo['applicationexpiredate']]
    #combo.to_csv('result.csv')
    return combo
