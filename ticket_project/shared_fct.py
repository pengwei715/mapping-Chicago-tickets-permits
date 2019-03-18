'''
link two dataframe together
We only care about the violation_code 'PARKING/STANDING PROHIBITED ANYTIME'
with full streetclosure during the permits time and in the permits area
'''

import pandas as pd
import re
from datetime import datetime
import numpy as np
import process_tickets
import permits

def link_permits_tickets(per, tik):
    '''
    Join two dataframe together based on the location and issue time
    
    Input:
        per: data frame of permits data
        tik: data frame of tickets data
    Return:
        combo: pandas dataframe contains the large joint table
    '''
    per = per[per['streetclosure'] == 'Full']
    tik = tik[tik['violation_code'] == 'NO STANDING/PARKING TIME RESTRICTED']
    tik['upper_streetname'] = tik.street_name.str.extract(r'(.+)\s.+\Z')
    tik['upper_streetname'] = tik.upper_streetname.str.upper()
    tik['upper_streetname'] = tik['upper_streetname'].astype('category')

    per = per.rename(index=str, columns = {'streetname': 'upper_streetname', 
                                             'direction': 'street_dir'})
    
    per.loc[:,'streetnumberfrom'] = np.floor(per['streetnumberfrom']/100)*100
    per.loc[:,'streetnumberto'] = np.ceil(per['streetnumberto']/100)*100

    tik = tik.reset_index()
    combo = tik.merge(per, on =['upper_streetname','street_dir'])

    combo= combo[combo['streetnumberfrom'] <= combo['street_num']]

    combo= combo[combo['streetnumberto'] >= combo['street_num']]
    combo= combo[combo['issue_date'] >= combo['applicationstartdate']]
    combo= combo[combo['issue_date'] <= combo['applicationexpiredate']]
    combo = combo.drop_duplicates('ticket_number')
    #combo.to_csv('result.csv')

    return combo

def go_tickets(parameters):
    '''
    Runs the program for the tickets dataset given the specified parameters

    Inputs:
    parameters (dictonary): dictionary mapping strings of parameter names to
        strings with parameter values
    '''
    column_dict = {'violation': 'violation_code',
                   'start_date': 'issue_date',
                   'end_date': 'issue_date',
                   'location': ['geocoded_lng', 'geocoded_lat'],
                   'neighborhood': 'zipcode'}
    if set(parameters.keys()) - set(column_dict.keys()):
        print('Error: Invalid parameter for tickets dataset!')
    else:
        print('Do the work for permits.')
        print('Loading the tickets dataset...')
        tickets = process_tickets.import_tickets(process_tickets.TICKETS_FILEPATH, 
                                                 process_tickets.VIOLATIONS_FILEPATH)
        #call filtering function
        print('Generating analysis...')
        #call map generating function

def go_permits(parameters):
    '''
    Runs the program for the tickets dataset given the specified parameters

    Inputs:
    parameters (dictonary): dictionary mapping strings of parameter names to
        strings with parameter values
    '''
    column_dict = {'worktype': 'worktype',
                   'start_date': 'applicationfinalizeddate',
                   'end_date': 'applicationfinalizeddate',
                   'location': ['longitude', 'latitude'],
                   'closing_type': 'streetclosure',
                   'streetname': 'streetname'}
    if set(parameters.keys()) - set(column_dict.keys()):
        print('Error: Invalid parameter for permits dataset!')
    else:
        print('Loading the permits dataset...')
        pers = permits.get_permits()
        #call permits filtering function
        print('Generating the analysis...')
        #call map generating function

def go_linked(parameters):
    '''
    Runs the program for the tickets dataset given the specified parameters

    Inputs:
    parameters (dictonary): dictionary mapping strings of parameter names to
        strings with parameter values
    '''
    column_dict = {'worktype': 'worktype',
                   'start_date': 'applicationfinalizeddate',
                   'end_date': 'applicationfinalizeddate',
                   'location': ['longitude', 'latitude'],
                   'neighborhood': 'zipcode'
                   'streetname': 'streetname'}
    if set(parameters.keys()) - set(column_dict.keys()):
        print('Error: Invalid parameter for linked dataset!')
    else:
        print('Loading the tickets dataset...')
        tickets = process_tickets.import_tickets(TICKETS_FILEPATH,
                                                 VIOLATIONS_FILEPATH)
        valid_tickets_params = set(['start_date', 'end_date', 'location',
                                    'neighborhood'])
        permits_params = {key: parameters[key] for key in valid_tickets_params
                          if key in column_dict}
        #need to filter tickets with appropriate parameters
        print('Loading the permits dataset...')
        pers = permits.get_permits()
        valid_permits_params = set(['worktype', 'start_date', 'end_date',
                                    'location', 'streetname'])
        permits_params = {key: parameters[key] for key in valid_permits_params
                          if key in column_dict}
        #call permits filter function
        print('Linking permits to tickets...')
        linked = link_permits_tickets(pers, tickets)
        print('Generating the analysis...')
        #call map generating fucntion

if __name__ == "__main__":
    usage = "python3 shrink_tickets.py <dataset> <parameters>"
    assert (len(sys.argv) == 3), ('Please specify what data set to use',
        '(tickets, permits, or links), and a JSON string specifying parameters')
    dataset = sys.argv[1]
    assert dataset in ['tickets', 'permits', 'linked'], ('Must specify', 
        'tickets or permits dataset in parameters.')
    parameters = json.loads(sys.argv[2])
    if dataset == 'tickets':
        go_tickets(parameters)
    elif dataset == 'permits':
        go_permits(parameters)
    elif dataset == 'linked':
        go_linked(parameters)
