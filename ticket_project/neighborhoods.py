import geopandas as geo_pd
import matplotlib.pyplot as plt
import descartes
import pandas as pd
import shapely
import pyproj #pyproj dependency is 1.9.6; check for update 2.0.0 is breaking
from sodapy import Socrata

def import_geometries():
    '''
    Imports the shapefiles from a given data store on the City of Chicago data
    portal and retuns a geopandas dataframe linking geometries to different
    attributes.

    Inputs:
    id (str): the data set identifier
    
    Returns: tuple of geopandas dataframes, the first containing zipcode data
        and the second containing neighborhood information
    '''
    client = Socrata('data.cityofchicago.org', 'SB7994tcuBpSSczrQvMx9N0Uy',
                     username="benfogarty@uchicago.edu", password='d5Nut6LrCHL&')

    files = client.get('unjd-c2ca')
    df = pd.DataFrame(zipcode_files)
    df['the_geom'] = df.the_geom.apply(shapely.geometry.shape)
    df = geo_pd.GeoDataFrame(df, geometry='the_geom')
    
    return df

def link_zipcodes_neighborhoods():
    '''
    Returns a dataframe that links zipcodes to neighborhoods. Each zipcode and
    each neighborhood may appear more than once in the resulting dataframe as
    zipcodes may intersect multiple neighborhoods and vice versa.

    Returns: pandas dataframe
    '''
    zipcodes = import_geometries('unjd-c2ca')
    neighborhoods = import_geometries('y6yq-dbs2')


def read_shapefiles(filepath):
    '''
    Reads the neighborhood shapefiles into a geopandas dataframe

    Inputs:
        filepath (str): the path to the neighborhood shapefiles

    Returns: geopandas dataframe describing neighborhoods
    '''
    return geo_pd.read_file(filepath)


def convert_to_geodf(df, long_col, lat_col, proj=None):
    '''
    Converts a regular pandas dataframe to a geopandas dataframe, based on
    coordinated in the regular pandas dataframe.

    Inputs:
        df (Pandas DataFrame): the dataframe to convert
        long_col (str): the name of the column containing longitude coordinates
        lat_col (str): the name of the column containing latitude coordinates
        proj (dict): the pprojection for the GeoDataFrame coordinates
    
    Returns (geopandas GeoDataFrame)
    '''
    if not proj:
        proj = {'init': 'epsg:4326'}
    
    df['coordinates'] = df.apply(lambda x: (x[long_col], x[lat_col]), axis=1)
    df = df[df[long_col].notna() & df[lat_col].notna()]
    df.loc[:,'coordinates'] = df['coordinates'].apply(shapely.geometry.Point)
    print('here')
    geodf = geo_pd.GeoDataFrame(df, geometry='coordinates')
    geodf.crs = proj
    return geodf


def find_neighborhoods(geo_df, neighborhoods):
    '''
    Performs a spatial join to link the entries in a GeoDataFrame with their
    respective neighborhoods

    Inputs:
        geo_df (GeoPandas GeoDataFrame): the geodataframe to link with
            neighborhoods
        neighborhoods (GeoPandas GeoDataFrame): a GeoDataFrame containing all
            the neighborhoods

    Returns: (GeoPandas GeoDataFrames)
    '''
    geo_df = geo_df.to_crs(neighborhoods.crs)
    merged = geo_pd.sjoin(geo_df, neighborhoods, how='inner', op='within')
    return merged

