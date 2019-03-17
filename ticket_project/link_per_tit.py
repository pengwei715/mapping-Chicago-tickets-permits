'''
link two dataframe together
We only care about the violation_code 'PARKING/STANDING PROHIBITED ANYTIME'
with full streetclosure during the permits time and in the permits area
'''

import pandas as pd
import re
from datetime import datetime
import numpy as np
import pdb

def link_permits_tickets(per, tik):
    '''
    Join two dataframe together based on the location and issue time
    
    Input:
        per: data frame of permits data
        tik: data frame of tickets data
    Return:
        combo: pandas dataframe contains the large joint table
    '''
    get = lambda m: re.match(r'(.+) (.+)$',m).group(1).upper()\
        if re.match(r'(.+) (.+)$',m) else None

    per = per[per['streetclosure'] == 'Full']
    tik = tik[tik['violation_code'] == 'NO STANDING/PARKING TIME RESTRICTED']
    tik['upper_streetname'] = tik['street_name'].apply(lambda m: get(m))
    tik['upper_streetname'] = tik['upper_streetname'].astype('category')

    per = per.rename(index=str, columns = {'streetname': 'upper_streetname', 
                                             'direction': 'street_dir'})
    
    per['streetnumberfrom'] = np.floor(per['streetnumberfrom']/100)*100
    per['streetnumberto'] = np.ceil(per['streetnumberto']/100)*100
    
    combo = tik.merge(per, on = ['upper_streetname','street_dir'])\
               .drop_duplicates('ticket_number')
    combo= combo[combo['streetnumberfrom'] <= combo['street_num']]
    combo= combo[combo['streetnumberto'] >= combo['street_num']]
    combo= combo[combo['issue_date'] >= combo['applicationstartdate']]
    combo= combo[combo['issue_date'] <= combo['applicationexpiredate']]
    combo.to_csv('result.csv')
    return combo

