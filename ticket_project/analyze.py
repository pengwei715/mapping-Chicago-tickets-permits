'''
Common functions of the whole software, can be used to analyze spatial
distributions of tickets, transportantion department permits, or linked
tickets and transportation department permits.
'''

import re
import json
import sys
import csv
from datetime import datetime
import pandas as pd
import numpy as np
import geocoder
import geopandas
import matplotlib as mpl
import matplotlib.pyplot as plt
import neighborhoods as nbhds
import data_loader


#columns of tickets data that users can filter
TICKET_COLUMNS = {'violation': 'violation_code',
                  'start_date': 'issue_date',
                  'end_date': 'issue_date',
                  'location': ['geocoded_lng', 'geocoded_lat'],
                  'neighborhood': 'zipcode'}
#columns of permits data that users can filter
PERMIT_COLUMNS = {'worktype': 'worktypedescription',
                  'start_date': 'applicationexpiredate',
                  'end_date': 'applicationexpiredate',
                  'location': ['longitude', 'latitude'],
                  'closing_type': 'streetclosure'}

def link_permits_tickets(per, tik):
    '''
    Filter the tickets on two violation type then
    Join two dataframe together based on the location and issue time

    Input:
        per: dataframe of permits data
        tik: dataframe of tickets data
    Return:
        combo: pandas dataframe contains the large joint table
    '''
    per = per[per['streetclosure'] == 'Full']

    tik = tik[tik.violation_code.isin(['NO STANDING/PARKING TIME RESTRICTED',
                                       'PARKING/STANDING PROHIBITED ANYTIME'])]\
                                .copy() #shallow copy, required to avoid warning

    tik['upper_streetname'] = tik.street_name.str.extract(r'(.+)\s.+\Z')
    tik['upper_streetname'] = tik.upper_streetname.str.upper()
    tik['upper_streetname'] = tik['upper_streetname'].astype('category')

    per = per.rename(index=str, columns={'streetname': 'upper_streetname',
                                         'direction': 'street_dir'})

    per.loc[:, 'streetnumberfrom'] = np.floor(per['streetnumberfrom']/100)*100
    per.loc[:, 'streetnumberto'] = np.ceil(per['streetnumberto']/100)*100

    tik = tik.reset_index()
    combo = tik.merge(per, on=['upper_streetname', 'street_dir'])

    combo = combo[combo['streetnumberfrom'] <= combo['street_num']]
    combo = combo[combo['streetnumberto'] >= combo['street_num']]
    combo = combo[combo['issue_date'] >= combo['applicationstartdate']]
    combo = combo[combo['issue_date'] <= combo['applicationexpiredate']]
    combo = combo.drop_duplicates('ticket_number')

    return combo

def link_with_neighborhoods(dataframe, lng_col, lat_col):
    '''
    Helper function to get a geocoded dataframe

    Input:
        df: dataframe with latitude and longtitude columns
    Return:
        A dataframe that is geocoded
    '''
    nbhd = nbhds.import_geometries(nbhds.NEIGHS_ID)
    geodf = nbhds.convert_to_geodf(dataframe, lng_col, lat_col)
    return nbhds.find_neighborhoods(geodf, nbhd)

def filter_input(df, input_dict, column_dict, db_type):
    '''
    Filter the data frame based on the input_dict

    Inputs:
        df: data frame to filter
        input_dict: dict of the user's input
    Returns:
        df: filtered df
    '''
    fail_str = '{} {} not found, ignoring'
    success_str = 'filtered on {} number of ' + db_type + ' reduced from {} to {}'
    dist_diff = 0.0145 #approximately 1 mile in distance

    for key, val in input_dict.items():
        row_nums = df.shape[0]

        if key in ('worktype', 'closing_type', 'streetname', 'violation'):
            unique_vals = df[column_dict[key]].unique()
            if val not in unique_vals:
                print(fail_str.format(key, val))
            else:
                df = df[df[column_dict[key]] == val]
                print(success_str.format(val, row_nums, df.shape[0]))
        if key == 'neighborhood' and 'neighborhood' in column_dict:
            neigh_dict = nbhds.link_neighs_zips()
            if val in neigh_dict:
                df = df[df[column_dict[key]].isin(neigh_dict[val])]
                print(success_str.format('zipcode approximation for ' + val, \
                                         row_nums, df.shape[0]))
            else:
                print(fail_str.format(key, val))
        if key in ('start_date', 'end_date'):
            if key == 'start_date':
                df = df[df[column_dict[key]] > val]
            else:
                df = df[df[column_dict[key]] < val]
            print(success_str.format(val, row_nums, df.shape[0]))
        if key == 'location':
            g = geocoder.osm(input_dict[key])
            if 'addr:city' not in g.osm or not g.osm['addr:city'] == 'Chicago':
                print('Geocoding failed to yield an address within Chicago.')
            if g.x and g.y:
                mask = (df[column_dict[key][0]] < g.x + dist_diff) & \
                       (df[column_dict[key][0]] > g.x - dist_diff) & \
                       (df[column_dict[key][1]] < g.y + dist_diff) & \
                       (df[column_dict[key][1]] > g.y - dist_diff)
                df = df[mask]
                print(success_str.format(\
                        'locations within approximately one mile of ' + val, \
                        row_nums, df.shape[0]))
            else:
                print(fail_str.format(key, val))
    return df

def project_onto_chicago(geodf, nbhd, location_bool, db_type, neighborhood=""):
    '''
    project_onto_chicago takes a set of parameters in the following form
    and returns any tickets which match

    Inputs: geodf (geodf): geopandas df
            nbhd (geodf): geopandas df
            location_bool (bool): True if location filters are present
            db_type (str): database name
            neighborhoods (str): the name of the neighborhood filter, if any

    References:
    Generating colormaps: https://towardsdatascience.com/lets-make-a-map-using-
        geopandas-pandas-andg-matplotlib-to-make-a-chloropleth-map-dddc31c1983d
    matplotlib docs: https://matplotlib.org/tutorials/colors/colormaps.html
    geopandas mapping docs: http://geopandas.org/mapping.html
    geopandas dissolve docs: http://geopandas.org/aggregation_with_dissolve.html
    geopandas merging docs: http://geopandas.org/mergingdata.html#spatial-joins
    '''
    if not geodf.empty:
        first_col = geodf.columns[0]
        if location_bool:
            if neighborhood:
                geodf = geodf[geodf['pri_neigh'] == neighborhood]
                print('Exact filtering on', neighborhood, geodf.shape[0], \
                        'tickets remain')
            nbhd = nbhd[nbhd['pri_neigh'].isin(geodf.pri_neigh.unique())]
            base = nbhd.plot(color='white', edgecolor='black')
            geodf.plot(ax=base)
            base.set_title('Regional ' + db_type + ' Map')

        else: #citywide
            fig, ax = plt.subplots(1)
            heat = geodf.dissolve(by='pri_neigh', aggfunc='count')
            heat = nbhd.merge(heat, on='pri_neigh', how='left').fillna(0)
            heat.plot(ax=ax, cmap='coolwarm', column=first_col, linewidth=0.8,
                      linestyle='-')
            ax.axis('off')
            ax.set_title('Chicago ' + db_type + ' Heat Map')
            n_min = min(heat[first_col])
            n_max = max(heat[first_col])
            leg = mpl.cm.ScalarMappable(cmap='coolwarm', norm=mpl.colors.Normalize(
                vmin=n_min, vmax=n_max))
            leg._A = []
            colorbar = fig.colorbar(leg)

        caption = str(geodf.shape[0]) + ' entries remain '
        if db_type != 'permits':
            caption = (caption + 'which represents $' + 
                       str(int(geodf['fine_amt'].agg('sum'))) + ' in fines')
        plt.figtext(0.5, 0.01, caption, wrap=True, horizontalalignment='center',
                    fontsize=12)
        plt.show()
    else:
        print('Dataset cannot be mapped because your search yields no matches.')


def go_tickets(parameters):
    '''
    Runs the program for the tickets dataset given the specified parameters

    Inputs:
    parameters (dictonary): dictionary mapping strings of parameter names to
        strings with parameter values
    '''
    location_bool = False

    if set(parameters.keys()) - set(TICKET_COLUMNS.keys()):
        print('Error: Invalid parameter for tickets dataset!')
    else:
        print('Loading the tickets dataset...')
        tickets = data_loader.import_tickets(data_loader.TICKETS_FILEPATH,
                                             data_loader.VIOLATIONS_FILEPATH)
        tickets = filter_input(tickets, parameters, TICKET_COLUMNS, 'tickets')
        
        print()
        print('Generating analysis...')
        nbhd = nbhds.import_geometries(nbhds.NEIGHS_ID)
        tickets = link_with_neighborhoods(tickets, 'geocoded_lng', 'geocoded_lat')
    if 'location' in parameters or 'neighborhood' in parameters:
        location_bool = True

    project_onto_chicago(tickets, nbhd, location_bool, 'tickets',
    	                 parameters.get('neighborhood', ""))


def go_permits(parameters):
    '''
    Runs the program for the tickets dataset given the specified parameters

    Inputs:
    parameters (dictonary): dictionary mapping strings of parameter names to
        strings with parameter values
    '''
    location_bool = False

    if set(parameters.keys()) - set(PERMIT_COLUMNS.keys()):
        print('Error: Invalid parameter for permits dataset!')
    else:
        print('Loading the permits dataset...')
        pers = data_loader.get_permits('07-13-2015')
        pers = filter_input(pers, parameters, PERMIT_COLUMNS, 'permits')
        
        print()
        print('Building the map...')
        nbhd = nbhds.import_geometries(nbhds.NEIGHS_ID)
        pers = link_with_neighborhoods(pers, 'longitude', 'latitude')
    
    if 'location' in parameters or 'neighborhood' in parameters:
        location_bool = True

    project_onto_chicago(pers, nbhd, location_bool, 'permits', 
                         parameters.get('neighborhood', ""))

def go_linked(parameters):
    '''
    Runs the program for the tickets dataset given the specified parameters

    Inputs:
    parameters (dictonary): dictionary mapping strings of parameter names to
        strings with parameter values
    '''
    location_bool = False

    if set(parameters.keys())- (set(PERMIT_COLUMNS.keys()) | 
                                set(TICKET_COLUMNS.keys())):
        print('Error: Invalid parameter for linked dataset!')
    else:
        print('Loading the tickets dataset...')
        tickets = data_loader.import_tickets(data_loader.TICKETS_FILEPATH,
                                             data_loader.VIOLATIONS_FILEPATH)
        tickets = filter_input(tickets, parameters, TICKET_COLUMNS, 'tickets')

        print()
        print('Loading the permits dataset...')
        pers = data_loader.get_permits('07-13-2015')
        pers = filter_input(pers, parameters, PERMIT_COLUMNS, 'permits')

        print()
        if not (pers.empty or tickets.empty):
            print('Linking permits to tickets...')
            linked = link_permits_tickets(pers, tickets)
            nbhd = nbhds.import_geometries(nbhds.NEIGHS_ID)
            linked = link_with_neighborhoods(linked, 'geocoded_lng',
                                             'geocoded_lat')
            
            print()
            print('Building the map...')
            if 'location' in parameters or 'neighborhood' in parameters:
                location_bool = True
            project_onto_chicago(linked, nbhd, location_bool, 'linked', \
                parameters.get('neighborhood', ""))
        else:
            print('Datasets cannot be linked because your search yields either', 
                   '0 tickets, 0 permits, or both.')


if __name__ == "__main__":
    usage = "python3 analyze.py <dataset> <parameters>"
    assert (len(sys.argv) == 3), \
           ('Please specify what data set to use',
            '(tickets, permits, or links)',
            'and a JSON string specifying parameters')
    dataset = sys.argv[1]
    assert dataset in ['tickets', 'permits', 'linked'], \
        ('Must specify', 'tickets or permits dataset in parameters.')
    parameters = json.loads(sys.argv[2])
    if dataset == 'tickets':
        go_tickets(parameters)
    elif dataset == 'permits':
        go_permits(parameters)
    elif dataset == 'linked':
        go_linked(parameters)
